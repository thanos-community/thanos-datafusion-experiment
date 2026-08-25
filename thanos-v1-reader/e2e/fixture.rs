use std::{
    fs::File,
    io,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

#[cfg(unix)]
use std::os::{fd::AsRawFd, unix::process::CommandExt};

use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tempfile::TempDir;
use tonic::transport::{Channel, Endpoint};

pub const MINT: i64 = 1_700_000_000_000;
pub const MAXT: i64 = 1_700_003_600_000;
pub const SAMPLE_COUNT: usize = 240;
pub const POD_COUNT: usize = 2;
pub const RESOLUTION_5M: i64 = 5 * 60 * 1000;

pub struct GeneratedFixture {
    root: Option<TempDir>,
    blocks: PathBuf,
}

pub fn generated_fixture() -> GeneratedFixture {
    let root = tempfile::Builder::new()
        .prefix("thanos-v1-reader-e2e-")
        .tempdir()
        .expect("create e2e fixture directory");
    let blocks = root.path().join("blocks");
    let generator_directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");
    let output = Command::new("go")
        .args([
            "run",
            ".",
            "--output",
            blocks.to_str().unwrap(),
            "--clean",
            "--mint",
            &MINT.to_string(),
            "--maxt",
            &MAXT.to_string(),
            "--samples",
            &SAMPLE_COUNT.to_string(),
            "--instances",
            "2",
            "--pods",
            &POD_COUNT.to_string(),
            "--routes",
            "2",
            "--native-series",
            "1",
            "--scalar-edge-cases",
            "--downsample-5m=true",
        ])
        .current_dir(generator_directory)
        .output()
        .expect("run deterministic Thanos fixture generator");
    assert!(
        output.status.success(),
        "Thanos fixture generator failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    GeneratedFixture {
        root: Some(root),
        blocks,
    }
}

impl GeneratedFixture {
    fn root(&self) -> &Path {
        self.root
            .as_ref()
            .expect("e2e fixture root is available")
            .path()
    }

    #[cfg(unix)]
    pub async fn start_reader(&self) -> ReaderProcess {
        let grpc_listener = TcpListener::bind("127.0.0.1:0").expect("reserve gRPC listener");
        let metrics_listener = TcpListener::bind("127.0.0.1:0").expect("reserve metrics listener");
        let address = grpc_listener.local_addr().expect("gRPC listener address");
        let metrics_address = metrics_listener
            .local_addr()
            .expect("metrics listener address");
        let address_text = address.to_string();
        let metrics_address_text = metrics_address.to_string();
        let config_path = self.root().join("reader.toml");
        std::fs::write(
            &config_path,
            format!(
                "listen_addr = {address_text:?}\nmetrics_listen_addr = {metrics_address_text:?}\nindex_cache_location = {:?}\n\n[[repositories]]\nname = \"e2e\"\nuri = {:?}\n",
                self.root().join("cache").display().to_string(),
                format!("file://{}", self.blocks.display()),
            ),
        )
        .expect("write reader config");
        let stderr_path = self.root().join("reader.stderr");
        let stderr = File::create(&stderr_path).expect("create reader stderr log");

        let grpc_fd = grpc_listener.as_raw_fd();
        let metrics_fd = metrics_listener.as_raw_fd();
        let mut command = Command::new(env!("CARGO_BIN_EXE_thanos-v1-reader"));
        command
            .env("THANOS_READER_CONFIG", &config_path)
            .env("THANOS_READER_LISTEN_FD", "3")
            .env("THANOS_READER_METRICS_LISTEN_FD", "4")
            .env("OTEL_SDK_DISABLED", "true")
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr));
        unsafe {
            command.pre_exec(move || {
                duplicate_listener(grpc_fd, 3).and_then(|_| duplicate_listener(metrics_fd, 4))
            });
        }
        let child = command.spawn().expect("start reader process");
        drop(grpc_listener);
        drop(metrics_listener);

        let reader = ReaderProcess {
            child,
            address: address.to_string(),
            stderr_path,
        };
        reader.wait_until_ready().await;
        reader
    }
}

#[cfg(unix)]
fn duplicate_listener(
    source: std::os::fd::RawFd,
    destination: std::os::fd::RawFd,
) -> io::Result<()> {
    if unsafe { libc::dup2(source, destination) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

impl Drop for GeneratedFixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(root) = self.root.take() {
                let path = root.keep();
                eprintln!("retained failed e2e fixture at {}", path.display());
            }
        }
    }
}

pub struct ReaderProcess {
    child: Child,
    address: String,
    stderr_path: PathBuf,
}

impl ReaderProcess {
    async fn client(&self) -> FlightSqlServiceClient<Channel> {
        Endpoint::from_shared(format!("http://{}", self.address))
            .expect("valid reader endpoint")
            .connect()
            .await
            .map(FlightSqlServiceClient::new)
            .expect("connect Flight SQL client")
    }

    async fn wait_until_ready(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(mut client) = Endpoint::from_shared(format!("http://{}", self.address))
                .expect("valid reader endpoint")
                .connect()
                .await
                .map(FlightSqlServiceClient::new)
            {
                if client.execute("SELECT 1".to_owned(), None).await.is_ok() {
                    return;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "reader did not become ready; stderr:\n{}",
                self.stderr(),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn query(&self, sql: &str) -> Vec<RecordBatch> {
        let mut client = self.client().await;
        let info = client
            .execute(sql.to_owned(), None)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "Flight SQL query failed for {sql:?}: {error}; stderr:\n{}",
                    self.stderr()
                )
            });
        assert_eq!(
            info.endpoint.len(),
            1,
            "query should produce one Flight endpoint"
        );
        let ticket = info.endpoint[0]
            .ticket
            .clone()
            .expect("Flight endpoint ticket");
        client
            .do_get(ticket)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "Flight SQL DoGet failed for {sql:?}: {error}; stderr:\n{}",
                    self.stderr()
                )
            })
            .try_collect()
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "Flight SQL stream failed for {sql:?}: {error}; stderr:\n{}",
                    self.stderr()
                )
            })
    }

    pub async fn query_error(&self, sql: &str) -> String {
        self.client()
            .await
            .execute(sql.to_owned(), None)
            .await
            .expect_err("query should fail")
            .to_string()
    }

    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path)
            .unwrap_or_else(|error| format!("read stderr: {error}"))
    }
}

impl Drop for ReaderProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("reader stderr:\n{}", self.stderr());
        }
    }
}
