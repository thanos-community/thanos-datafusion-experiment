This package implements [custom table provider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html) for thanos storage [format](https://thanos.io/tip/thanos/storage.md/)

It starts by defining the objstore, which can be s3:// or file://. Underlying access pattern is using [OpenDAL](https://docs.rs/opendal/latest/opendal/) library which handles fetching, concurrency, retry, caching, etc. 

Any thanos block reader is using OpenDAL wrapper, and async via tokio. Tokio is used within datafusion, thus it's important for interoperability here.