fn main() {
    prost_build::compile_protos(
        &["src/models/value.proto", "src/models/row.proto"],
        &["src/models"],
    )
    .unwrap();
}
