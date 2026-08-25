# thanos-datafusion-experiment

## Local Rust build cache

Cargo is configured to use `sccache` when it is installed locally, falling back
to plain `rustc` otherwise. Install it to reuse compiled Rust artifacts across
small local rebuilds:

```sh
cargo install sccache
```
