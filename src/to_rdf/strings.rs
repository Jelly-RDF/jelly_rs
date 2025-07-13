use std::borrow::Cow;

use crate::Deserializer;
use crate::deserialize::ToTerm as _;
use crate::error::DeserializeError;
use crate::lookup::LookupType;
use crate::proto::{RdfIri, RdfLiteral, RdfTriple, rdf_literal::LiteralKind};

use super::ToRdf;

static EMPTY: Cow<'static, str> = Cow::Borrowed("");
pub struct StringRdf;
impl ToRdf for StringRdf {
    type Term = String;

    type Triple<'a> = (&'a str, &'a str, &'a str);

    type Quad<'a> = (&'a str, &'a str, &'a str, Option<&'a str>);
    type State = ();

    fn default_term() -> Self::Term {
        String::new()
    }

    #[inline]
    fn iri(
        iri: RdfIri,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        Ok(format!(
            "<{}{}>",
            deserializer
                .prefix_table
                .get(iri.prefix_id, LookupType::Stay)
                .unwrap_or(&EMPTY),
            deserializer.name_table.get(iri.name_id, LookupType::Inc)?
        ))
    }

    #[inline]
    fn bnode(key: String, _: &mut Deserializer<Self>) -> Result<Self::Term, DeserializeError> {
        Ok(format!("_:B{}", key))
    }

    #[inline]
    fn literal(
        literal: RdfLiteral,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let lex = literal.lex;
        Ok(match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => format!("\"{}\"@{}", lex, tag),
            Some(LiteralKind::Datatype(tag)) => {
                format!(
                    "\"{}\"^^<{}>",
                    lex,
                    deserializer.datatype_table.get(tag, LookupType::Invalid)?
                )
            }
            None => {
                format!("\"{}\"", lex)
            }
        })
    }

    #[inline]
    fn term_triple(
        triple: RdfTriple,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let RdfTriple {
            subject,
            predicate,
            object,
        } = triple;
        let s = if let Some(s) = subject {
            deserializer.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermInTermTriple);
        };

        let p = if let Some(s) = predicate {
            deserializer.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermInTermTriple);
        };

        let o = if let Some(s) = object {
            deserializer.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermInTermTriple);
        };

        Ok(format!("<< {} {} {} >>", s, p, o))
    }

    #[inline]
    fn triple<'a>(
        deserializer: &'a mut Deserializer<Self>,
    ) -> Result<Self::Triple<'a>, DeserializeError> {
        Ok((
            &deserializer
                .last_subject
                .as_ref()
                .expect("subject to be present"),
            &deserializer
                .last_predicate
                .as_ref()
                .expect("predicate to be present"),
            &deserializer
                .last_object
                .as_ref()
                .expect("object to be present"),
        ))
    }

    #[inline]
    fn quad<'a>(
        deserializer: &'a mut Deserializer<Self>,
    ) -> Result<Self::Quad<'a>, DeserializeError> {
        Ok((
            &deserializer
                .last_subject
                .as_ref()
                .expect("subject to be present"),
            &deserializer
                .last_predicate
                .as_ref()
                .expect("predicate to be present"),
            &deserializer
                .last_object
                .as_ref()
                .expect("object to be present"),
            deserializer.last_graph.as_ref().map(|x| x.as_str()),
        ))
    }
}
