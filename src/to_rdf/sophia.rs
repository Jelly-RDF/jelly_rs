use std::{borrow::Cow, collections::HashMap, sync::Arc};

use sophia_api::term::{BnodeId, IriRef, LanguageTag};
use sophia_term::ArcTerm;

use crate::{
    Inner,
    deserialize::ToTerm as _,
    error::DeserializeError,
    lookup::LookupType,
    proto::{RdfIri, RdfLiteral, RdfTriple, rdf_literal::LiteralKind},
};

use super::ToRdf;

const DEFAULT_DATA_TYPE: &'static str = "";

static EMPTY: Cow<'static, str> = Cow::Borrowed("");
pub struct SophiaRdf;
impl ToRdf for SophiaRdf {
    type Term = ArcTerm;

    type Triple<'b> = [ArcTerm; 3];

    type Quad<'b> = ([ArcTerm; 3], Option<ArcTerm>);

    type State = HashMap<String, ArcTerm>;

    fn default_term() -> Self::Term {
        ArcTerm::Iri(IriRef::new_unchecked(Arc::from("")))
    }

    fn iri(iri: RdfIri, deserializer: &mut Inner<Self>) -> Result<Self::Term, DeserializeError> {
        let iri = format!(
            "{}{}",
            deserializer
                .prefix_table
                .get(iri.prefix_id, LookupType::Stay)
                .unwrap_or(&EMPTY),
            deserializer.name_table.get(iri.name_id, LookupType::Inc)?
        );
        Ok(ArcTerm::Iri(IriRef::new_unchecked(Arc::from(iri))))
    }

    fn bnode(key: String, deserializer: &mut Inner<Self>) -> Result<Self::Term, DeserializeError> {
        Ok(deserializer
            .state
            .entry(key)
            .or_insert_with_key(|key| {
                ArcTerm::BlankNode(BnodeId::new_unchecked(Arc::from(format!("b{}", key))))
            })
            .clone())
    }

    fn literal(
        literal: RdfLiteral,
        deserializer: &mut Inner<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let lex = Arc::from(literal.lex);
        Ok(match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => {
                let lang = LanguageTag::new_unchecked(Arc::from(tag));
                ArcTerm::Literal(sophia_term::GenericLiteral::LanguageString(lex, lang))
            }
            Some(LiteralKind::Datatype(tag)) => {
                let datatype = IriRef::new_unchecked(Arc::from(
                    deserializer
                        .datatype_table
                        .get(tag, LookupType::Invalid)?
                        .to_string(),
                ));

                ArcTerm::Literal(sophia_term::GenericLiteral::Typed(lex, datatype))
            }
            None => {
                let datatype = IriRef::new_unchecked(Arc::from(DEFAULT_DATA_TYPE));

                ArcTerm::Literal(sophia_term::GenericLiteral::Typed(lex, datatype))
            }
        })
    }

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

        Ok(ArcTerm::Triple(Arc::from([s, p, o])))
    }

    fn triple<'b>(d: &'b mut Inner<Self>) -> Result<Self::Triple<'b>, DeserializeError> {
        Ok([
            d.last_subject.clone().expect("subject"),
            d.last_predicate.clone().expect("predicate"),
            d.last_object.clone().expect("object"),
        ])
    }

    fn quad<'b>(d: &'b mut Inner<Self>) -> Result<Self::Quad<'b>, DeserializeError> {
        Ok((
            [
                d.last_subject.clone().expect("subject"),
                d.last_predicate.clone().expect("predicate"),
                d.last_object.clone().expect("object"),
            ],
            d.last_graph.clone(),
        ))
    }
}
