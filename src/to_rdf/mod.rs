use crate::Inner;
use crate::error::DeserializeError;
use crate::proto::{RdfIri, RdfLiteral, RdfTriple};

mod strings;
pub use strings::StringRdf;

#[cfg(feature = "sophia")]
mod sophia;
#[cfg(feature = "sophia")]
pub use sophia::SophiaRdf;

pub trait ToRdf: Sized {
    type Term;
    type Triple<'b>
    where
        Self: 'b;

    type Quad<'b>
    where
        Self: 'b;
    type State: Default;

    fn default_term() -> Self::Term;

    fn iri(iri: RdfIri, deserializer: &mut Inner<Self>) -> Result<Self::Term, DeserializeError>;
    fn bnode(key: String, deserializer: &mut Inner<Self>) -> Result<Self::Term, DeserializeError>;
    fn literal(
        literal: RdfLiteral,
        deserializer: &mut Inner<Self>,
    ) -> Result<Self::Term, DeserializeError>;
    fn term_triple(
        triple: RdfTriple,
        deserializer: &mut Inner<Self>,
    ) -> Result<Self::Term, DeserializeError>;

    fn triple<'b>(deserializer: &'b mut Inner<Self>) -> Result<Self::Triple<'b>, DeserializeError>;
    fn quad<'b>(deserializer: &'b mut Inner<Self>) -> Result<Self::Quad<'b>, DeserializeError>;
}
