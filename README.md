# RepriseDB

[![Rust](https://github.com/emersonmde/reprisedb/actions/workflows/rust.yml/badge.svg)](https://github.com/emersonmde/reprisedb/actions/workflows/rust.yml)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

RepriseDB is an in-development, disk-persistent key-value store written in Rust. It's designed with an LSM Tree-based architecture, optimizing the interplay between in-memory and on-disk data. This project is still in the early stages of development and is not recommended for production use.

## Design

RepriseDB's operation relies on two primary data structures:

- `MemTable`: an in-memory B-Tree map, providing rapid access and mutable operations. It temporarily stores data before its transition to an `SSTable` when it reaches a specific size limit.

- `SSTable` (Sorted String Table): a disk-resident structure which stores key-value pairs persistently. These pairs are encoded using Protocol Buffers and sorted to enable efficient binary search during key lookups.

## LSM Tree Compaction

RepriseDB handles LSM Tree compaction using a merge strategy. When the number of `SSTable`s grows, a compaction process is initiated to prevent inefficiencies during key lookups. This process, managed by the `compact_sstables` method, merges the two most recent `SSTable`s into one, with a "last write wins" strategy for key collisions.

## Getting Started

RepriseDB is still in development. For early experimentation, add this to your `Cargo.toml`:

```toml
[dependencies]
reprisedb = { git = "https://github.com/emersonmde/reprisedb.git", branch = "main" }
```

## Todo
- Speed up compaction using multiple workers
- Add a write-ahead log to support crash recovery
    - Sequential writes to the log and MemTable simultaneously
    - When the MemTable is full, replace MemTable and log with new ones
        - Set to 4 KB
    - Trigger MemTable flush to disk, delete old MemTable and log
- Handle shutdown gracefully, stopping reads/writes before flushing
- Add multi-layer compaction strategy
  - Change compaction to keep tiers of data based on modification date
  - SSTables should stay below 64MB
- Add an MANIFEST file for SSTables
- Add API
- Sharding RwLocks on the MemTable or the SSTable vec for better performance

## License

This project is licensed under the [MIT license](LICENSE).