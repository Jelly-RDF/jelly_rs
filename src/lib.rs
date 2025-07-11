#[macro_use]
extern crate log;

use lookup::{Lookup, LookupType};
use proto::{
    RdfDatatypeEntry, RdfIri, RdfLiteral, RdfNameEntry, RdfPrefixEntry, RdfQuad, RdfStreamOptions,
    RdfTriple, rdf_literal::LiteralKind, rdf_quad as q, rdf_triple as t,
};

use paste::paste;

// These functions are the same for all 6 types, I don't want to write that by hand
// Only difference is the q::Subject::SIri vs q::Predicate::PIri
trait ToTerm<Term> {
    fn to_term(&mut self, thing: Term) -> String;
}
macro_rules! implTerm {
    ($k:path, $letter:ident, $($extra:tt)?) => {
        paste! {
                impl ToTerm<$k> for Thing {
                    fn to_term(&mut self, thing: $k) -> String {
                        match thing {
                            $k::[<$letter Iri>](rdf_iri) => self.iri(rdf_iri),
                            $k::[<$letter Bnode>](bnode) => self.bnode(bnode),
                            $k::[<$letter Literal>](rdf_literal) => self.literal(rdf_literal),
                            $k::[<$letter TripleTerm>](rdf_triple) => self.term_triple($($extra)? rdf_triple),
                        }
                    }
                }
        }
    };
}

implTerm!(q::Subject, S,);
implTerm!(q::Predicate, P,);
implTerm!(q::Object, O,);
implTerm!(t::Subject, S, *);
implTerm!(t::Predicate, P, *);
implTerm!(t::Object, O, *);

pub mod lookup;
pub mod proto;

pub struct Thing {
    name_table: Lookup,
    prefix_table: Lookup,
    datatype_table: Lookup,

    last_subject: String,
    last_predicate: String,
    last_object: String,
    last_graph: Option<String>,
}

impl Thing {
    pub fn from_options(options: &RdfStreamOptions) -> Self {
        info!("Options {:?}", options);
        Self {
            name_table: Lookup::new(options.max_name_table_size),
            datatype_table: Lookup::new(options.max_datatype_table_size),
            prefix_table: Lookup::new(options.max_prefix_table_size),

            last_subject: String::new(),
            last_predicate: String::new(),
            last_object: String::new(),
            last_graph: None,
        }
    }

    pub fn prefix_entry(&mut self, prefix: RdfPrefixEntry) {
        self.prefix_table.set(prefix.id, prefix.value);
    }

    pub fn name_entry(&mut self, name: RdfNameEntry) {
        self.name_table.set(name.id, name.value);
    }

    pub fn datatype_entry(&mut self, entry: RdfDatatypeEntry) {
        self.datatype_table.set(entry.id, entry.value);
    }

    pub fn iri(&mut self, iri: RdfIri) -> String {
        format!(
            "<{}{}>",
            self.prefix_table.get(iri.prefix_id, LookupType::Stay),
            self.name_table.get(iri.name_id, LookupType::Inc)
        )
    }

    fn bnode(&mut self, key: ::prost::alloc::string::String) -> String {
        format!("_:B{}", key)
    }

    fn literal(&mut self, literal: RdfLiteral) -> String {
        let lex = literal.lex;
        match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => format!("\"{}\"@{}", lex, tag),
            Some(LiteralKind::Datatype(tag)) => {
                format!(
                    "\"{}\"^^<{}>",
                    lex,
                    self.datatype_table.get(tag, LookupType::Invalid)
                )
            }
            None => {
                format!("\"{}\"", lex)
            }
        }
    }

    fn term_triple(&mut self, triple: RdfTriple) -> String {
        let RdfTriple {
            subject,
            predicate,
            object,
        } = triple;
        let s = if let Some(s) = subject {
            self.to_term(s)
        } else {
            info!("I don't know if this is correct");
            self.last_subject.clone()
        };

        let p = if let Some(s) = predicate {
            self.to_term(s)
        } else {
            info!("I don't know if this is correct");
            self.last_predicate.clone()
        };

        let o = if let Some(s) = object {
            self.to_term(s)
        } else {
            info!("I don't know if this is correct");
            self.last_object.clone()
        };

        format!("<< {} {} {} >>", s, p, o)
    }

    pub fn q_graph(&mut self, sub: q::Graph) {
        trace!("q_graph");
        match sub {
            q::Graph::GIri(iri) => self.last_graph = Some(self.iri(iri)),
            q::Graph::GBnode(str) => self.last_graph = Some(self.bnode(str)),
            q::Graph::GLiteral(literal) => self.last_graph = Some(self.literal(literal)),
            q::Graph::GDefaultGraph(_) => self.last_graph = None,
        }
    }

    pub fn triple<'a>(&'a mut self, triple: RdfTriple) -> (&'a str, &'a str, &'a str) {
        if let Some(subject) = triple.subject {
            self.last_subject = self.to_term(subject);
        }

        if let Some(predicate) = triple.predicate {
            self.last_predicate = self.to_term(predicate);
        }

        if let Some(object) = triple.object {
            self.last_object = self.to_term(object);
        }

        (&self.last_subject, &self.last_predicate, &self.last_object)
    }

    pub fn quad<'a>(&'a mut self, quad: RdfQuad) -> (&'a str, &'a str, &'a str, Option<&'a str>) {
        if let Some(subject) = quad.subject {
            self.last_subject = self.to_term(subject);
        }

        if let Some(predicate) = quad.predicate {
            self.last_predicate = self.to_term(predicate);
        }

        if let Some(object) = quad.object {
            self.last_object = self.to_term(object);
        }

        if let Some(graph) = quad.graph {
            self.q_graph(graph);
        }
        (
            &self.last_subject,
            &self.last_predicate,
            &self.last_object,
            self.last_graph.as_ref().map(|x| x.as_str()),
        )
    }
}
