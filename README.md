# RepriseDB
RepreiseDB log-structured merge-tree (LSM tree) database engine

The database uses an in memory BTreeMap as the initial stage and a Sorted
String Table (SSTable) as the second stage. The SSTable is a sorted file
that uses ProtoBuf to serialize the data.
