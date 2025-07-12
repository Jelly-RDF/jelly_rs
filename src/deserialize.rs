use crate::lookup::Lookup;
use crate::proto::{
    RdfDatatypeEntry, RdfNameEntry, RdfPrefixEntry, RdfQuad, RdfStreamOptions, RdfTriple,
    rdf_quad as q, rdf_triple as t,
};
use crate::to_rdf::ToRdf;
use paste::paste;

// These functions are the same for all 6 types, I don't want to write that by hand
// Only difference is the q::Subject::SIri vs q::Predicate::PIri
pub trait ToTerm<Term, TermType> {
    fn to_term(&mut self, thing: Term) -> TermType;
}
macro_rules! implTerm {
    ($k:path, $letter:ident, $($extra:tt)?) => {
        paste! {
                impl<T: ToRdf> ToTerm<$k, T::Term> for Deserializer<T> {
                    #[inline]
                    fn to_term(&mut self, thing: $k) -> T::Term {
                        match thing {
                            $k::[<$letter Iri>](rdf_iri) => T::iri(rdf_iri, self),
                            $k::[<$letter Bnode>](bnode) => T::bnode(bnode, self),
                            $k::[<$letter Literal>](rdf_literal) => T::literal(rdf_literal, self),
                            $k::[<$letter TripleTerm>](rdf_triple) => T::term_triple($($extra)? rdf_triple, self),
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

pub struct Deserializer<T: ToRdf> {
    pub name_table: Lookup,
    pub prefix_table: Lookup,
    pub datatype_table: Lookup,

    pub last_subject: Option<T::Term>,
    pub last_predicate: Option<T::Term>,
    pub last_object: Option<T::Term>,
    pub last_graph: Option<T::Term>,

    pub state: T::State,
}

impl<T: ToRdf> Deserializer<T> {
    pub fn from_options(options: &RdfStreamOptions) -> Self {
        info!("Options {:?}", options);
        Self {
            name_table: Lookup::new(options.max_name_table_size),
            datatype_table: Lookup::new(options.max_datatype_table_size),
            prefix_table: Lookup::new(options.max_prefix_table_size),

            last_subject: None,
            last_predicate: None,
            last_object: None,
            last_graph: None,

            state: T::State::default(),
        }
    }

    #[inline]
    pub fn prefix_entry(&mut self, prefix: RdfPrefixEntry) {
        self.prefix_table.set(prefix.id, prefix.value);
    }

    #[inline]
    pub fn name_entry(&mut self, name: RdfNameEntry) {
        self.name_table.set(name.id, name.value);
    }

    #[inline]
    pub fn datatype_entry(&mut self, entry: RdfDatatypeEntry) {
        self.datatype_table.set(entry.id, entry.value);
    }

    #[inline]
    pub fn q_graph(&mut self, sub: q::Graph) {
        trace!("q_graph");
        match sub {
            q::Graph::GIri(iri) => self.last_graph = Some(T::iri(iri, self)),
            q::Graph::GBnode(str) => self.last_graph = Some(T::bnode(str, self)),
            q::Graph::GLiteral(literal) => self.last_graph = Some(T::literal(literal, self)),
            q::Graph::GDefaultGraph(_) => self.last_graph = None,
        }
    }

    #[inline]
    pub fn triple<'a>(&'a mut self, triple: RdfTriple) -> T::Triple<'a> {
        if let Some(subject) = triple.subject {
            self.last_subject = Some(self.to_term(subject));
        }

        if let Some(predicate) = triple.predicate {
            self.last_predicate = Some(self.to_term(predicate));
        }

        if let Some(object) = triple.object {
            self.last_object = Some(self.to_term(object));
        }

        T::triple(self)
    }

    #[inline]
    pub fn quad<'a>(&'a mut self, quad: RdfQuad) -> T::Quad<'a> {
        if let Some(subject) = quad.subject {
            self.last_subject = Some(self.to_term(subject));
        }

        if let Some(predicate) = quad.predicate {
            self.last_predicate = Some(self.to_term(predicate));
        }

        if let Some(object) = quad.object {
            self.last_object = Some(self.to_term(object));
        }

        if let Some(graph) = quad.graph {
            self.q_graph(graph);
        }
        T::quad(self)
    }
}
