use crate::error::{ConfigError, DeserializeError, MessageType, PhysicalStreamError, TermLocation};
use crate::lookup::Lookup;

use crate::proto::rdf_quad::Graph;
use crate::proto::rdf_stream_row::Row;
use crate::proto::{
    PhysicalStreamType, RdfDatatypeEntry, RdfNameEntry, RdfPrefixEntry, RdfQuad, RdfStreamFrame,
    RdfStreamOptions, RdfTriple, rdf_graph_start as gs, rdf_quad as q, rdf_triple as t,
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

    physical_type: PhysicalStreamType,
    graph_started: bool,
}

impl<T: ToRdf> Inner<T> {
    pub fn from_options(options: &RdfStreamOptions) -> Result<Self, ConfigError> {
        info!("Options {:?}", options);
        let physical_type = PhysicalStreamType::try_from(options.physical_type)?;
        if physical_type == PhysicalStreamType::Unspecified {
            return Err(ConfigError::InvalidPhysicalType(physical_type));
        }
        if let Some(table_error) = ConfigError::name_table(options.max_name_table_size) {
            return Result::Err(table_error);
        };

        if let Some(table_error) = ConfigError::prefix_table(options.max_prefix_table_size) {
            return Result::Err(table_error);
        };

        if let Some(table_error) = ConfigError::datatype_table(options.max_datatype_table_size) {
            return Result::Err(table_error);
        };

        Ok(Self {
            name_table: Lookup::new(options.max_name_table_size),
            datatype_table: Lookup::new(options.max_datatype_table_size),
            prefix_table: Lookup::new(options.max_prefix_table_size),

            last_subject: None,
            last_predicate: None,
            last_object: None,
            last_graph: None,

            state: T::State::default(),

            physical_type,

            graph_started: false,
        })
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
    pub fn triple_with_graph<'a>(
        &'a mut self,
        triple: RdfTriple,
    ) -> Result<T::Quad<'a>, DeserializeError> {
        if let Some(subject) = triple.subject {
            self.last_subject = Some(self.to_term(subject)?);
        }

        if let Some(predicate) = triple.predicate {
            self.last_predicate = Some(self.to_term(predicate)?);
        }

        if let Some(object) = triple.object {
            self.last_object = Some(self.to_term(object)?);
        }

        T::quad(self)
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

impl<T: ToRdf> Clone for Inner<T>
  where T: ToOwned,
        <T as ToRdf>::State : ToOwned<Owned = <T as ToRdf>::State>,
        <T as ToRdf>::Term : ToOwned<Owned = <T as ToRdf>::Term>
{
    fn clone(&self) -> Self {
        Self {
            name_table: self.name_table.clone(),
            datatype_table: self.datatype_table.clone(),
            prefix_table: self.prefix_table.clone(),
            last_subject: match &self.last_subject {
                Some(s) => Some(s.to_owned()),
                None => None
            },
            last_predicate: match &self.last_predicate {
                Some(p) => Some(p.to_owned()),
                None => None
            },
            last_object: match &self.last_object {
                Some(o) => Some(o.to_owned()),
                None => None
            },
            last_graph: match &self.last_graph {
                Some(g) => Some(g.to_owned()),
                None => None
            },
            state: (&self.state).to_owned(),
            physical_type: self.physical_type,
            graph_started: self.graph_started
        }
    }
}

pub trait RdfHandler<T: ToRdf> {
    fn handle_triple<'b>(&mut self, triple: T::Triple<'b>);
    fn handle_quad<'b>(&mut self, quad: T::Quad<'b>);
}

pub struct StateHandler<S, FT, FQ> {
    pub state: S,
    ft: FT,
    fq: FQ,
}
impl<S, FT, FQ> StateHandler<S, FT, FQ> {
    pub fn new(state: S, ft: FT, fq: FQ) -> Self {
        Self { state, ft, fq }
    }
}
impl<S, FT, FQ, T: ToRdf> RdfHandler<T> for StateHandler<S, FT, FQ>
where
    FT: for<'a> FnMut(T::Triple<'a>, &mut S),
    FQ: for<'a> FnMut(T::Quad<'a>, &mut S),
{
    fn handle_triple<'b>(&mut self, triple: <T as ToRdf>::Triple<'b>) {
        (self.ft)(triple, &mut self.state);
    }

    fn handle_quad<'b>(&mut self, quad: <T as ToRdf>::Quad<'b>) {
        (self.fq)(quad, &mut self.state);
    }
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
    ) -> Result<H, DeserializeError> {
        let rows = frame.rows.into_iter().flat_map(|x| x.row);

        for row in rows {
            debug!("Row {:?}", row);
            if let Row::Options(options) = &row {
                match self {
                    Deserializer::Inited(_) => {
                        info!("Didn't expect new options, but I don't care, ignoring");
                    }
                    Deserializer::Empty => {
                        *self = Deserializer::Inited(Inner::from_options(&options)?);
                    }
                }
            }

            let thing = match self {
                Deserializer::Inited(deserializer) => deserializer,
                Deserializer::Empty => {
                    return Err(DeserializeError::ConfigError(ConfigError::NotSet));
                }
            };

            match row {
                Row::Options(_) => {}
                Row::Triple(rdf_triple) => {
                    if thing.physical_type == PhysicalStreamType::Graphs {
                        if !thing.graph_started {
                            return Err(DeserializeError::PhysicalStreamError(
                                PhysicalStreamError::NotYetSet {
                                    detected: thing.physical_type,
                                    expected: MessageType::GraphStart,
                                },
                            ));
                        } else {
                            handler.handle_quad(thing.triple_with_graph(rdf_triple)?);
                        }
                    } else if thing.physical_type == PhysicalStreamType::Triples {
                        handler.handle_triple(thing.triple(rdf_triple)?)
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: thing.physical_type,
                                incoming: MessageType::Triple,
                            },
                        ));
                    }
                }
                Row::Quad(rdf_quad) => {
                    println!("Quad");
                    if thing.physical_type == PhysicalStreamType::Quads {
                        handler.handle_quad(thing.quad(rdf_quad)?)
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: thing.physical_type,
                                incoming: MessageType::Quad,
                            },
                        ));
                    }
                }
                Row::GraphStart(rdf_graph_start) => {
                    println!("Graph start! {:?}", rdf_graph_start.graph);

                    if thing.physical_type == PhysicalStreamType::Graphs {
                        let g = match rdf_graph_start.graph {
                            Some(gs::Graph::GIri(iri)) => Graph::GIri(iri),
                            Some(gs::Graph::GDefaultGraph(iri)) => Graph::GDefaultGraph(iri),
                            Some(gs::Graph::GBnode(iri)) => Graph::GBnode(iri),
                            Some(gs::Graph::GLiteral(iri)) => Graph::GLiteral(iri),
                            None => {
                                return Result::Err(DeserializeError::MissingTerm(
                                    TermLocation::Graph,
                                ));
                            }
                        };
                        thing.q_graph(g)?;
                        thing.graph_started = true;
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: thing.physical_type,
                                incoming: MessageType::GraphStart,
                            },
                        ));
                    }
                }
                Row::GraphEnd(_) => {
                    if thing.physical_type == PhysicalStreamType::Graphs {
                        println!("Graph end!");
                        thing.last_graph = None;
                        thing.graph_started = false;
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: thing.physical_type,
                                incoming: MessageType::GraphEnd,
                            },
                        ));
                    }
                }
                Row::Namespace(rdf_namespace_declaration) => {
                    info!("Name space is fine: {} ", rdf_namespace_declaration.name,);
                }
                Row::Name(rdf_name_entry) => thing.name_entry(rdf_name_entry)?,
                Row::Prefix(rdf_prefix_entry) => thing.prefix_entry(rdf_prefix_entry)?,
                Row::Datatype(rdf_datatype_entry) => thing.datatype_entry(rdf_datatype_entry)?,
            }
        }
        Ok(handler)
    }
}

impl<T: ToRdf> Default for Deserializer<T> {
    fn default() -> Self {
        Deserializer::Empty
    }
}

impl<T: ToRdf> Clone for Deserializer<T>
where T: ToOwned,
  <T as ToRdf>::State : ToOwned<Owned = <T as ToRdf>::State>,
  <T as ToRdf>::Term : ToOwned<Owned = <T as ToRdf>::Term>
{
    fn clone(&self) -> Self {
        match self {
            Deserializer::Inited(inner) => Deserializer::Inited(inner.to_owned()),
            Deserializer::Empty => Deserializer::Empty,
        }
    }
}
