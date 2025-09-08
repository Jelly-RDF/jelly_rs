use std::borrow::Cow;

use crate::deserialize::{Deserializer, RdfHandler};
use crate::error::{DeserializeError, LookupError};
use crate::proto::{RdfIri, RdfLiteral, RdfTriple, rdf_literal::LiteralKind};

use super::{IriParams, LiteralParams, ToRdf};
pub struct StringRdf;

type T = Cow<'static, str>;
impl ToRdf for StringRdf {
    type Term = T;

    type Triple = (T, T, T);

    type Quad = (T, T, T, Option<T>);
    type State = ();

    fn default_term() -> Self::Term {
        Cow::Borrowed("")
    }

    #[inline]
    fn iri(
        _iri: RdfIri,
        params: IriParams,
        _deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        Ok(format!("<{}{}>", params.prefix, params.name,).into())
    }

    #[inline]
    fn bnode(key: String, _: &mut Deserializer<Self>) -> Result<Self::Term, DeserializeError> {
        Ok(format!("_:B{}", key).into())
    }

    #[inline]
    fn literal(
        literal: RdfLiteral,
        param: LiteralParams,
        _deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let lex = literal.lex;
        Ok(match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => format!("\"{}\"@{}", lex, tag),
            Some(LiteralKind::Datatype(_)) => {
                format!(
                    "\"{}\"^^<{}>",
                    lex,
                    param.datatype.ok_or(LookupError::InvalidLookupAction)?
                )
            }
            None => {
                format!("\"{}\"", lex)
            }
        }
        .into())
    }

    #[inline]
    fn term_triple(
        triple: RdfTriple,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let (s, p, o) = deserializer.triple_term_terms(triple)?;
        Ok(format!("<< {} {} {} >>", s, p, o).into())
    }

    #[inline]
    fn triple(deserializer: &mut Deserializer<Self>) -> Result<Self::Triple, DeserializeError> {
        let (s, p, o) = deserializer.spo()?;
        Ok((s, p, o))
    }

    #[inline]
    fn quad(deserializer: &mut Deserializer<Self>) -> Result<Self::Quad, DeserializeError> {
        let (s, p, o) = deserializer.spo()?;
        Ok((s, p, o, deserializer.last_graph.as_ref().map(|x| x.clone())))
    }
}

impl<'a> RdfHandler<StringRdf> for &mut Vec<(String, String, String, Option<String>)> {
    fn handle_triple<'b>(&mut self, (s, p, o): <StringRdf as ToRdf>::Triple) {
        self.push((s.to_string(), p.to_string(), o.to_string(), None));
    }

    fn handle_quad<'b>(&mut self, (s, p, o, q): <StringRdf as ToRdf>::Quad) {
        self.push((
            s.to_string(),
            p.to_string(),
            o.to_string(),
            q.map(String::from),
        ));
    }
}
