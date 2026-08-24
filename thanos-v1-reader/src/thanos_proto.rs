pub mod thanos {
    tonic::include_proto!("thanos");

    pub mod info {
        tonic::include_proto!("thanos.info");
    }
}

pub mod prometheus_copy {
    tonic::include_proto!("prometheus_copy");
}

pub mod hintspb {
    tonic::include_proto!("hintspb");
}
