use std::{collections::HashMap, sync::Arc};

use sophia_api::term::{BnodeId, IriRef, LanguageTag, language_tag::InvalidLanguageTag};
use sophia_term::ArcTerm;

use crate::{
    deserialize::Deserializer,
    error::DeserializeError,
    proto::{RdfIri, RdfLiteral, RdfTriple, rdf_literal::LiteralKind},
};

use super::{IriParams, LiteralParams, ToRdf};

const DEFAULT_DATA_TYPE: &'static str = "http://www.w3.org/2001/XMLSchema#string";

pub struct SophiaRdf;
impl ToRdf for SophiaRdf {
    type Term = ArcTerm;

    type Triple = [ArcTerm; 3];
    type Quad = ([ArcTerm; 3], Option<ArcTerm>);

    type State = HashMap<String, ArcTerm>;

    fn default_term() -> Self::Term {
        ArcTerm::Iri(IriRef::new_unchecked(Arc::from("")))
    }

    fn iri(
        _iri: RdfIri,
        params: IriParams,
        _deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let iri = format!("{}{}", params.prefix, params.name);
        Ok(ArcTerm::Iri(IriRef::new_unchecked(Arc::from(iri))))
    }

    fn bnode(
        key: String,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
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
        params: LiteralParams,
        _deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let lex = Arc::from(literal.lex);
        Ok(match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => {
                let lang = LanguageTag::new(Arc::from(tag))?;
                ArcTerm::Literal(sophia_term::GenericLiteral::LanguageString(lex, lang))
            }
            Some(LiteralKind::Datatype(_)) => {
                let datatype_value = params
                    .datatype
                    .ok_or(DeserializeError::InvalidLanguageTag(InvalidLanguageTag(
                        String::from("missing language tag"),
                    )))?
                    .to_string();

                let datatype = IriRef::new(Arc::from(datatype_value))?;

                ArcTerm::Literal(sophia_term::GenericLiteral::Typed(lex, datatype))
            }
            None => {
                let datatype = IriRef::new(Arc::from(DEFAULT_DATA_TYPE))?;
                ArcTerm::Literal(sophia_term::GenericLiteral::Typed(lex, datatype))
            }
        })
    }

    fn term_triple(
        triple: RdfTriple,
        deserializer: &mut Deserializer<Self>,
    ) -> Result<Self::Term, DeserializeError> {
        let (s, p, o) = deserializer.triple_term_terms(triple)?;
        Ok(ArcTerm::Triple(Arc::from([s, p, o])))
    }

    fn triple(d: &mut Deserializer<Self>) -> Result<Self::Triple, DeserializeError> {
        let (s, p, o) = d.spo()?;
        Ok([s, p, o])
    }

    fn quad(d: &mut Deserializer<Self>) -> Result<Self::Quad, DeserializeError> {
        let (s, p, o) = d.spo()?;
        Ok(([s, p, o], d.last_graph.clone()))
    }
}
