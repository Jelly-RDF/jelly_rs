use std::borrow::Cow;

use crate::deserialize::Deserializer;
use crate::error::DeserializeError;
use crate::proto::{RdfIri, RdfLiteral, RdfTriple};

mod strings;
pub use strings::StringRdf;

#[cfg(feature = "sophia")]
mod sophia;
#[cfg(feature = "sophia")]
pub use sophia::SophiaRdf;

pub struct IriParams {
    pub prefix: Cow<'static, str>,
    pub name: Cow<'static, str>,
}

pub struct LiteralParams {
    pub datatype: Option<Cow<'static, str>>,
}

pub trait ToRdf: Sized {
    type Term;
    type Triple;
    type Quad;

    type State;

    fn default_term() -> Self::Term;

    fn iri(
        iri: RdfIri,
        params: IriParams,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError>;
    fn bnode(
        key: String,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError>;
    fn literal(
        literal: RdfLiteral,
        params: LiteralParams,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError>;
    fn term_triple(
        triple: RdfTriple,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError>;

    fn triple(deserializer: &mut Deserializer<Self>) -> Result<Self::Triple, DeserializeError>;
    fn quad(deserializer: &mut Deserializer<Self>) -> Result<Self::Quad, DeserializeError>;
}
