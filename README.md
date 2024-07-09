# RethDB

A thread-safe implementation of revm `Database` that directly fetch data from Reth database.

## Usage

```rust

let db = RethDB::new(
    Chain::mainnet(), "path/to/reth/db", Some(block_number)
);

// use it in the same way as other revm DBs.
```

## Performance

For replaying blocks, approximately 40x faster than AlloyDB using IPC and 100x faster than AlloyDB using HTTP RPC.