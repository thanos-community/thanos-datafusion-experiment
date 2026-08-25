use criterion::{Criterion, criterion_group, criterion_main};
use thanos_v1_reader::config::{CachePolicy, ChunkCacheConfig};

fn cache_size_parsing(criterion: &mut Criterion) {
    let config = ChunkCacheConfig {
        directory: std::env::temp_dir(),
        max_size: "10GiB".to_owned(),
        page_size: "16KiB".to_owned(),
        policy: CachePolicy::Slru,
        protected_fraction: 0.8,
    };
    criterion.bench_function("chunk_cache_configuration", |bencher| {
        bencher.iter(|| {
            assert_eq!(config.max_size_bytes().unwrap(), 10 * 1024_u64.pow(3));
            assert_eq!(config.page_size_bytes().unwrap(), 16 * 1024);
        });
    });
}

criterion_group!(benches, cache_size_parsing);
criterion_main!(benches);
