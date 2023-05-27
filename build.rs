fn main() {
    prost_build::compile_protos(&["src/models/row.proto"], &["src/"]).unwrap();
}