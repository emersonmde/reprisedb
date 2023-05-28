# RepriseDB

RepriseDB is a disk-persistent key-value store, designed to facilitate rapid data access. It employs a Log-Structured
Merge (LSM) Tree-based architecture for effective data handling, optimizing the interplay between in-memory and on-disk
data.

## Overview

RepriseDB's core operation relies on two data structures: `MemTable`, an in-memory B-Tree map, and `SSTable`, a
disk-resident Sorted String Table. The `MemTable` temporarily stores data prior to its transition to an `SSTable` once a
specified size limit is reached. `SSTable`'s key-value pairs are encoded using Protocol Buffers and sorted to enable
binary search for key lookups in the future.

## LSM Tree Compaction and Merge Strategy

RepriseDB's LSM Tree-based compaction and merge strategy is central to its performance. As the number of `SSTable`s
increases, key lookup may necessitate scanning multiple tables. To prevent inefficiency, RepriseDB compacts `SSTable`s,
merging them into a single table.

This process, managed by the `compact_sstables` method, merges the two most recent `SSTable`s into one. In the event of
key collisions, the newer value is retained, implementing a "last write wins" approach. Should a compaction error occur,
RepriseDB reverts to the pre-compaction state, ensuring data consistency and integrity.

## Key Components

- **database.rs**: Functions as the main RepriseDB interface. Handles data reading and writing operations and initiates
  compaction when required.
- **sstable.rs**: Establishes the `SSTable` structure and operations, and includes an iterator for key-value pair
  traversal.

## License

RepriseDB is distributed under the MIT license.
