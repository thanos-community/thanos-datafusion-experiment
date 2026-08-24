use std::{pin::Pin, str};

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::FlightService,
    sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery, server::FlightSqlService},
};
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt, stream};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

/// A minimal Flight SQL service backed by a DataFusion session.
///
/// Statement query tickets use the SQL text as their opaque handle. Production services should
/// replace this with an expiring server-side query handle.
#[derive(Clone)]
pub struct DataFusionFlightService {
    context: SessionContext,
    location: String,
}

impl DataFusionFlightService {
    pub fn new(context: SessionContext, location: impl Into<String>) -> Self {
        Self {
            context,
            location: location.into(),
        }
    }

    fn query_from_handle(handle: &[u8]) -> Result<&str, Status> {
        let query = str::from_utf8(handle)
            .map_err(|_| Status::invalid_argument("statement handle must contain UTF-8 SQL"))?;

        if query.trim().is_empty() {
            return Err(Status::invalid_argument("SQL query must not be empty"));
        }

        Ok(query)
    }
}

#[tonic::async_trait]
impl FlightSqlService for DataFusionFlightService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status>
    {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };

        Ok(Response::new(Box::pin(stream::iter([Ok(response)]))))
    }

    async fn get_flight_info_statement(
        &self,
        statement: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        if statement.query.trim().is_empty() {
            return Err(Status::invalid_argument("SQL query must not be empty"));
        }

        let dataframe = self
            .context
            .sql(&statement.query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let ticket = TicketStatementQuery {
            statement_handle: statement.query.into(),
        };
        let endpoint = FlightEndpoint::new()
            .with_ticket(Ticket::new(ticket.as_any().encode_to_vec()))
            .with_location(self.location.clone());
        let flight_info = FlightInfo::new()
            .try_with_schema(dataframe.schema().as_arrow())
            .map_err(|error| Status::internal(error.to_string()))?
            .with_descriptor(request.into_inner())
            .with_endpoint(endpoint);

        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let query = Self::query_from_handle(&ticket.statement_handle)?;
        let batches = self
            .context
            .sql(query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            .collect()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        let batch_stream = stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map(|result| result.map_err(|error| Status::internal(error.to_string())));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &arrow_flight::sql::SqlInfo) {}
}
