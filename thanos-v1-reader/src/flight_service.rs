use std::str;

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService,
};
use datafusion::prelude::SessionContext;
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use tonic::{Request, Response, Status, Streaming};

/// A minimal Arrow Flight service that interprets command descriptors and tickets as SQL.
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

    fn query_from_bytes(bytes: &[u8]) -> Result<&str, Status> {
        let query = str::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("query must be valid UTF-8 SQL"))?;

        if query.trim().is_empty() {
            return Err(Status::invalid_argument("query must not be empty"));
        }

        Ok(query)
    }
}

#[tonic::async_trait]
impl FlightService for DataFusionFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("authentication is not configured"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented(
            "this server accepts SQL through get_flight_info",
        ))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let query = Self::query_from_bytes(&descriptor.cmd)?.to_owned();
        let dataframe = self
            .context
            .sql(&query)
            .await
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let flight_info = FlightInfo::new()
            .try_with_schema(dataframe.schema().as_arrow())
            .map_err(|error| Status::internal(error.to_string()))?
            .with_descriptor(descriptor)
            .with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(query))
                    .with_location(self.location.clone()),
            );

        Ok(Response::new(flight_info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("polling queries is not configured"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "request query metadata through get_flight_info",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let query = Self::query_from_bytes(&ticket.ticket)?;
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
            .map(|result| result.map_err(|error| Status::internal(error.to_string())))
            .boxed();

        Ok(Response::new(flight_stream))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("uploading data is not configured"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Flight actions are not configured"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Flight actions are not configured"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "bidirectional exchange is not configured",
        ))
    }
}
