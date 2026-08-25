#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    thanos_v1_reader::run().await
}
