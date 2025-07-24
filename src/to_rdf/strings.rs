use std::borrow::Cow;

use crate::Inner;
use crate::deserialize::{RdfHandler, ToTerm as _};
use crate::error::{DeserializeError, TermLocation};
use crate::lookup::LookupType;
use crate::proto::{RdfIri, RdfLiteral, RdfTriple, rdf_literal::LiteralKind};

use super::ToRdf;

static EMPTY: Cow<'static, str> = Cow::Borrowed("http://www.w3.org/2001/XMLSchema#string");
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
    fn iri(iri: RdfIri, deserializer: &mut Inner<Self>) -> Result<Self::Term, DeserializeError> {
        Ok(format!(
            "<{}{}>",
            deserializer
                .prefix_table
                .get(iri.prefix_id, LookupType::Stay)?,
            deserializer.name_table.get(iri.name_id, LookupType::Inc)?
        ))
    }

    #[inline]
    fn bnode(key: String, _: &mut Inner<Self>) -> Result<Self::Term, DeserializeError> {
        Ok(format!("_:B{}", key))
    }

    #[inline]
    fn literal(
        literal: RdfLiteral,
        deserializer: &mut Inner<Self>,
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
        deserializer: &mut Inner<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let RdfTriple {
            subject,
            predicate,
            object,
        } = triple;
        let s = if let Some(s) = subject {
            deserializer.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermTermTriple(
                TermLocation::Subject,
            ));
        };

        let p = if let Some(s) = predicate {
            deserializer.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermTermTriple(
                TermLocation::Predicate,
            ));
        };

        let o = if let Some(s) = object {
            deserializer.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermTermTriple(
                TermLocation::Object,
            ));
        };

        Ok(format!("<< {} {} {} >>", s, p, o))
    }

    #[inline]
    fn triple<'a>(deserializer: &'a mut Inner<Self>) -> Result<Self::Triple<'a>, DeserializeError> {
        Ok((
            &deserializer
                .last_subject
                .as_ref()
                .ok_or(DeserializeError::MissingTerm(TermLocation::Subject))?,
            &deserializer
                .last_predicate
                .as_ref()
                .ok_or(DeserializeError::MissingTerm(TermLocation::Predicate))?,
            &deserializer
                .last_object
                .as_ref()
                .ok_or(DeserializeError::MissingTerm(TermLocation::Object))?,
        ))
    }

    #[inline]
    fn quad<'a>(deserializer: &'a mut Inner<Self>) -> Result<Self::Quad<'a>, DeserializeError> {
        Ok((
            &deserializer
                .last_subject
                .as_ref()
                .ok_or(DeserializeError::MissingTerm(TermLocation::Subject))?,
            &deserializer
                .last_predicate
                .as_ref()
                .ok_or(DeserializeError::MissingTerm(TermLocation::Predicate))?,
            &deserializer
                .last_object
                .as_ref()
                .ok_or(DeserializeError::MissingTerm(TermLocation::Object))?,
            deserializer.last_graph.as_ref().map(|x| x.as_str()),
        ))
    }
}

impl<'a> RdfHandler<StringRdf> for &mut Vec<(String, String, String, Option<String>)> {
    fn handle_triple<'b>(&mut self, (s, p, o): <StringRdf as ToRdf>::Triple<'b>) {
        self.push((s.to_string(), p.to_string(), o.to_string(), None));
    }

    fn handle_quad<'b>(&mut self, (s, p, o, q): <StringRdf as ToRdf>::Quad<'b>) {
        self.push((
            s.to_string(),
            p.to_string(),
            o.to_string(),
            q.map(String::from),
        ));
    }
}
