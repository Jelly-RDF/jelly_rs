use std::{collections::HashMap, sync::Arc};

use sophia_api::term::{BnodeId, IriRef, LanguageTag};
use sophia_term::ArcTerm;

use crate::{
    Deserializer,
    deserialize::ToTerm as _,
    lookup::LookupType,
    proto::{RdfIri, RdfLiteral, RdfTriple, rdf_literal::LiteralKind},
};

use super::ToRdf;

const DEFAULT_DATA_TYPE: &'static str = "";

pub struct SophiaRdf;
impl ToRdf for SophiaRdf {
    type Term = ArcTerm;

    type Triple<'b> = [ArcTerm; 3];

    type Quad<'b> = ([ArcTerm; 3], Option<ArcTerm>);

    type State = HashMap<String, ArcTerm>;

    fn default_term() -> Self::Term {
        ArcTerm::Iri(IriRef::new_unchecked(Arc::from("")))
    }

    fn iri(iri: RdfIri, deserializer: &mut Deserializer<Self>) -> Self::Term {
        let iri = format!(
            "{}{}",
            deserializer
                .prefix_table
                .get(iri.prefix_id, LookupType::Stay),
            deserializer.name_table.get(iri.name_id, LookupType::Inc)
        );
        ArcTerm::Iri(IriRef::new_unchecked(Arc::from(iri)))
    }

    fn bnode(key: String, deserializer: &mut Deserializer<Self>) -> Self::Term {
        deserializer
            .state
            .entry(key)
            .or_insert_with_key(|key| {
                ArcTerm::BlankNode(BnodeId::new_unchecked(Arc::from(format!("b{}", key))))
            })
            .clone()
    }

    fn literal(literal: RdfLiteral, deserializer: &mut Deserializer<Self>) -> Self::Term {
        let lex = Arc::from(literal.lex);
        match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => {
                let lang = LanguageTag::new_unchecked(Arc::from(tag));
                ArcTerm::Literal(sophia_term::GenericLiteral::LanguageString(lex, lang))
            }
            Some(LiteralKind::Datatype(tag)) => {
                let datatype = IriRef::new_unchecked(Arc::from(
                    deserializer
                        .datatype_table
                        .get(tag, LookupType::Invalid)
                        .to_string(),
                ));

                ArcTerm::Literal(sophia_term::GenericLiteral::Typed(lex, datatype))
            }
            None => {
                let datatype = IriRef::new_unchecked(Arc::from(DEFAULT_DATA_TYPE));

                ArcTerm::Literal(sophia_term::GenericLiteral::Typed(lex, datatype))
            }
        }
    }

    fn term_triple(triple: RdfTriple, deserializer: &mut Deserializer<Self>) -> Self::Term {
        let RdfTriple {
            subject,
            predicate,
            object,
        } = triple;
        let s = if let Some(s) = subject {
            deserializer.to_term(s)
        } else {
            info!("I don't know if this is correct");
            todo!()
        };

        let p = if let Some(s) = predicate {
            deserializer.to_term(s)
        } else {
            info!("I don't know if this is correct");
            todo!()
        };

        let o = if let Some(s) = object {
            deserializer.to_term(s)
        } else {
            info!("I don't know if this is correct");
            todo!()
        };

        ArcTerm::Triple(Arc::from([s, p, o]))
    }

    fn triple<'b>(d: &'b mut Deserializer<Self>) -> Self::Triple<'b> {
        [
            d.last_subject.clone().expect("subject"),
            d.last_predicate.clone().expect("predicate"),
            d.last_object.clone().expect("object"),
        ]
    }

    fn quad<'b>(d: &'b mut Deserializer<Self>) -> Self::Quad<'b> {
        (
            [
                d.last_subject.clone().expect("subject"),
                d.last_predicate.clone().expect("predicate"),
                d.last_object.clone().expect("object"),
            ],
            d.last_graph.clone(),
        )
    }
}
