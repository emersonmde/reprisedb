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

In case of a compaction error, RepriseDB rolls back to the pre-compaction state, ensuring data consistency and integrity.

## Code Overview

- **database.rs**: The main interface of RepriseDB, manages data read/write operations and initiates compaction when required.
- **sstable.rs**: Defines the `SSTable` structure and related operations, and includes an iterator for key-value pair traversal.

## Getting Started

RepriseDB is still in development. For early experimentation, add this to your `Cargo.toml`:

```toml
[dependencies]
reprisedb = { git = "https://github.com/emersonmde/reprisedb.git", branch = "main" }
```

## Todo
- Add API
- Create sparse indexes of SSTables to allow for binary search
- Add multi-layer compaction strategy to better support time series writes
- Add a bloom filter for each of the SSTables and MemTable to speed up lookups
- Add a write-ahead log to support crash recovery

## Documentation

Additional documentation for methods and structures is in progress.

## Contributing

Feedback and contributions are very welcome at this early stage of development. Please feel free to submit issues or open pull requests.

## License

This project is licensed under the [MIT license](LICENSE).

## Maintainer

RepriseDB is developed and maintained by [Matthew Emerson](https://github.com/emersonmde).