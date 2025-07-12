use crate::lookup::{Lookup, LookupType};
use crate::proto::{
    RdfDatatypeEntry, RdfIri, RdfLiteral, RdfNameEntry, RdfPrefixEntry, RdfQuad, RdfStreamOptions,
    RdfTriple, rdf_literal::LiteralKind, rdf_quad as q, rdf_triple as t,
};
use paste::paste;

// These functions are the same for all 6 types, I don't want to write that by hand
// Only difference is the q::Subject::SIri vs q::Predicate::PIri
trait ToTerm<Term, TermType> {
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

pub trait ToRdf: Sized {
    type Term: Default;
    type Triple<'b>
    where
        Self: 'b;

    type Quad<'b>
    where
        Self: 'b;
    type State: Default;

    fn iri(iri: RdfIri, deserializer: &mut Deserializer<Self>) -> Self::Term;
    fn bnode(key: String, deserializer: &mut Deserializer<Self>) -> Self::Term;
    fn literal(literal: RdfLiteral, deserializer: &mut Deserializer<Self>) -> Self::Term;
    fn term_triple(triple: RdfTriple, deserializer: &mut Deserializer<Self>) -> Self::Term;

    fn triple<'b>(deserializer: &'b mut Deserializer<Self>) -> Self::Triple<'b>;
    fn quad<'b>(deserializer: &'b mut Deserializer<Self>) -> Self::Quad<'b>;
}

pub struct StringRdf;
impl ToRdf for StringRdf {
    type Term = String;

    type Triple<'a> = (&'a str, &'a str, &'a str);

    type Quad<'a> = (&'a str, &'a str, &'a str, Option<&'a str>);
    type State = ();

    #[inline]
    fn iri(iri: RdfIri, deserializer: &mut Deserializer<Self>) -> Self::Term {
        format!(
            "<{}{}>",
            deserializer
                .prefix_table
                .get(iri.prefix_id, LookupType::Stay),
            deserializer.name_table.get(iri.name_id, LookupType::Inc)
        )
    }

    #[inline]
    fn bnode(key: String, _: &mut Deserializer<Self>) -> Self::Term {
        format!("_:B{}", key)
    }

    #[inline]
    fn literal(literal: RdfLiteral, deserializer: &mut Deserializer<Self>) -> Self::Term {
        let lex = literal.lex;
        match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => format!("\"{}\"@{}", lex, tag),
            Some(LiteralKind::Datatype(tag)) => {
                format!(
                    "\"{}\"^^<{}>",
                    lex,
                    deserializer.datatype_table.get(tag, LookupType::Invalid)
                )
            }
            None => {
                format!("\"{}\"", lex)
            }
        }
    }

    #[inline]
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

        format!("<< {} {} {} >>", s, p, o)
    }

    #[inline]
    fn triple<'a>(deserializer: &'a mut Deserializer<Self>) -> Self::Triple<'a> {
        (
            &deserializer.last_subject,
            &deserializer.last_predicate,
            &deserializer.last_object,
        )
    }

    #[inline]
    fn quad<'a>(deserializer: &'a mut Deserializer<Self>) -> Self::Quad<'a> {
        (
            &deserializer.last_subject,
            &deserializer.last_predicate,
            &deserializer.last_object,
            deserializer.last_graph.as_ref().map(|x| x.as_str()),
        )
    }
}

implTerm!(q::Subject, S,);
implTerm!(q::Predicate, P,);
implTerm!(q::Object, O,);
implTerm!(t::Subject, S, *);
implTerm!(t::Predicate, P, *);
implTerm!(t::Object, O, *);

pub struct Deserializer<T: ToRdf> {
    name_table: Lookup,
    prefix_table: Lookup,
    datatype_table: Lookup,

    last_subject: T::Term,
    last_predicate: T::Term,
    last_object: T::Term,
    last_graph: Option<T::Term>,
}

impl<T: ToRdf> Deserializer<T> {
    pub fn from_options(options: &RdfStreamOptions) -> Self {
        info!("Options {:?}", options);
        Self {
            name_table: Lookup::new(options.max_name_table_size),
            datatype_table: Lookup::new(options.max_datatype_table_size),
            prefix_table: Lookup::new(options.max_prefix_table_size),

            last_subject: T::Term::default(),
            last_predicate: T::Term::default(),
            last_object: T::Term::default(),
            last_graph: None,
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
            self.last_subject = self.to_term(subject);
        }

        if let Some(predicate) = triple.predicate {
            self.last_predicate = self.to_term(predicate);
        }

        if let Some(object) = triple.object {
            self.last_object = self.to_term(object);
        }

        T::triple(self)
    }

    #[inline]
    pub fn quad<'a>(&'a mut self, quad: RdfQuad) -> T::Quad<'a> {
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
        T::quad(self)
    }
}
