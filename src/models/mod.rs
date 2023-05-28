include!(concat!(env!("OUT_DIR"), "/_.rs"));

pub trait ValueKindSize {
    fn size(&self) -> usize;
}

impl ValueKindSize for value::Kind {
    fn size(&self) -> usize {
        match self {
            value::Kind::Int(_) => std::mem::size_of::<i64>(),
            value::Kind::Float(_) => std::mem::size_of::<f64>(),
            value::Kind::Str(s) => s.len(),
            value::Kind::Bytes(b) => b.len(),
        }
    }
}