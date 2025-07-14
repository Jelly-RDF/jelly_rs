use crate::error::DeserializeError;
use crate::lookup::Lookup;
use crate::proto::rdf_stream_row::Row;
use crate::proto::{
    RdfDatatypeEntry, RdfNameEntry, RdfPrefixEntry, RdfQuad, RdfStreamFrame, RdfStreamOptions,
    RdfTriple, rdf_quad as q, rdf_triple as t,
};
use crate::to_rdf::ToRdf;
use paste::paste;

// These functions are the same for all 6 types, I don't want to write that by hand
// Only difference is the q::Subject::SIri vs q::Predicate::PIri
pub trait ToTerm<Term, TermType> {
    fn to_term(&mut self, thing: Term) -> Result<TermType, DeserializeError>;
}

macro_rules! implTerm {
    ($k:path, $letter:ident, $($extra:tt)?) => {
        paste! {
                impl<T: ToRdf> ToTerm<$k, T::Term> for Inner<T> {
                    #[inline]
                    fn to_term(&mut self, thing: $k) -> Result<T::Term, DeserializeError> {
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

pub struct Inner<T: ToRdf> {
    pub name_table: Lookup,
    pub prefix_table: Lookup,
    pub datatype_table: Lookup,

    pub last_subject: Option<T::Term>,
    pub last_predicate: Option<T::Term>,
    pub last_object: Option<T::Term>,
    pub last_graph: Option<T::Term>,

    pub state: T::State,
}

impl<T: ToRdf> Inner<T> {
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
    pub fn prefix_entry(&mut self, prefix: RdfPrefixEntry) -> Result<(), DeserializeError> {
        self.prefix_table.set(prefix.id, prefix.value)?;
        Ok(())
    }

    #[inline]
    pub fn name_entry(&mut self, name: RdfNameEntry) -> Result<(), DeserializeError> {
        self.name_table.set(name.id, name.value)?;
        Ok(())
    }

    #[inline]
    pub fn datatype_entry(&mut self, entry: RdfDatatypeEntry) -> Result<(), DeserializeError> {
        self.datatype_table.set(entry.id, entry.value)?;
        Ok(())
    }

    #[inline]
    pub fn q_graph(&mut self, sub: q::Graph) -> Result<(), DeserializeError> {
        trace!("q_graph");
        match sub {
            q::Graph::GIri(iri) => self.last_graph = Some(T::iri(iri, self)?),
            q::Graph::GBnode(str) => self.last_graph = Some(T::bnode(str, self)?),
            q::Graph::GLiteral(literal) => self.last_graph = Some(T::literal(literal, self)?),
            q::Graph::GDefaultGraph(_) => self.last_graph = None,
        }
        Ok(())
    }

    #[inline]
    pub fn triple<'a>(&'a mut self, triple: RdfTriple) -> Result<T::Triple<'a>, DeserializeError> {
        if let Some(subject) = triple.subject {
            self.last_subject = Some(self.to_term(subject)?);
        }

        if let Some(predicate) = triple.predicate {
            self.last_predicate = Some(self.to_term(predicate)?);
        }

        if let Some(object) = triple.object {
            self.last_object = Some(self.to_term(object)?);
        }

        T::triple(self)
    }

    #[inline]
    pub fn quad<'a>(&'a mut self, quad: RdfQuad) -> Result<T::Quad<'a>, DeserializeError> {
        if let Some(subject) = quad.subject {
            self.last_subject = Some(self.to_term(subject)?);
        }

        if let Some(predicate) = quad.predicate {
            self.last_predicate = Some(self.to_term(predicate)?);
        }

        if let Some(object) = quad.object {
            self.last_object = Some(self.to_term(object)?);
        }

        if let Some(graph) = quad.graph {
            self.q_graph(graph)?;
        }

        T::quad(self)
    }
}

pub trait RdfHandler<T: ToRdf> {
    fn handle_triple<'b>(&mut self, triple: T::Triple<'b>);
    fn handle_quad<'b>(&mut self, quad: T::Quad<'b>);
}

impl<FT, FQ, T: ToRdf> RdfHandler<T> for (FT, FQ)
where
    FT: for<'a> FnMut(T::Triple<'a>),
    FQ: for<'a> FnMut(T::Quad<'a>),
{
    fn handle_triple<'b>(&mut self, triple: <T as ToRdf>::Triple<'b>) {
        self.0(triple)
    }

    fn handle_quad<'b>(&mut self, quad: <T as ToRdf>::Quad<'b>) {
        self.1(quad)
    }
}

impl<FT, FQ, T: ToRdf> RdfHandler<T> for &mut (FT, FQ)
where
    FT: for<'a> FnMut(T::Triple<'a>),
    FQ: for<'a> FnMut(T::Quad<'a>),
{
    fn handle_triple<'b>(&mut self, triple: <T as ToRdf>::Triple<'b>) {
        self.0(triple)
    }

    fn handle_quad<'b>(&mut self, quad: <T as ToRdf>::Quad<'b>) {
        self.1(quad)
    }
}

pub enum Deserializer<T: ToRdf> {
    Inited(Inner<T>),
    Empty,
}

impl<T: ToRdf> Deserializer<T> {
    pub fn new() -> Self {
        Deserializer::Empty
    }
    pub fn handle_frame<H: RdfHandler<T>>(
        &mut self,
        frame: RdfStreamFrame,
        mut handler: H,
    ) -> Result<(), DeserializeError> {
        let rows = frame.rows.into_iter().flat_map(|x| x.row);

        for row in rows {
            debug!("Row {:?}", row);
            if let Row::Options(options) = &row {
                match self {
                    Deserializer::Inited(_) => {
                        info!("Didn't expect new options, but I don't care, ignoring");
                    }
                    Deserializer::Empty => {
                        *self = Deserializer::Inited(Inner::from_options(&options));
                    }
                }
            }

            let thing = match self {
                Deserializer::Inited(deserializer) => deserializer,
                Deserializer::Empty => {
                    return Err(DeserializeError::MissingTermInTermTriple);
                }
            };

            match row {
                Row::Options(_) => {}
                Row::Triple(rdf_triple) => handler.handle_triple(thing.triple(rdf_triple)?),
                Row::Quad(rdf_quad) => handler.handle_quad(thing.quad(rdf_quad)?),
                Row::GraphStart(rdf_graph_start) => {
                    // TODO: use graph start
                }
                Row::GraphEnd(rdf_graph_end) => {
                    // TODO: use graph end
                }
                Row::Namespace(rdf_namespace_declaration) => {
                    info!("Name space is fine: {} ", rdf_namespace_declaration.name,);
                }
                Row::Name(rdf_name_entry) => {
                    if let Err(e) = thing.name_entry(rdf_name_entry) {
                        println!("Error {:?}", e)
                    }
                }
                Row::Prefix(rdf_prefix_entry) => {
                    if let Err(e) = thing.prefix_entry(rdf_prefix_entry) {
                        println!("Error {:?}", e)
                    }
                }
                Row::Datatype(rdf_datatype_entry) => {
                    if let Err(e) = thing.datatype_entry(rdf_datatype_entry) {
                        println!("Error {:?}", e)
                    }
                }
            }
        }
        Ok(())
    }
}
