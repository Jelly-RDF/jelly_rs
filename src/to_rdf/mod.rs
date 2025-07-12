use crate::Deserializer;
use crate::proto::{RdfIri, RdfLiteral, RdfTriple};

pub mod strings;
pub use strings::StringRdf;

pub trait ToRdf: Sized {
    type Term: Default;
    type Triple<'b>
    where
        Self: 'b;

    type Quad<'b>
    where
        Self: 'b;
    type State: Default;

    fn iri(iri: RdfIri, deserializer: &mut Deserializer<Self>) -> Self::Term;
    fn bnode(key: String, deserializer: &mut Deserializer<Self>) -> Self::Term;
    fn literal(literal: RdfLiteral, deserializer: &mut Deserializer<Self>) -> Self::Term;
    fn term_triple(triple: RdfTriple, deserializer: &mut Deserializer<Self>) -> Self::Term;

    fn triple<'b>(deserializer: &'b mut Deserializer<Self>) -> Self::Triple<'b>;
    fn quad<'b>(deserializer: &'b mut Deserializer<Self>) -> Self::Quad<'b>;
}
