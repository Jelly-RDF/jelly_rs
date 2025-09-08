use crate::error::{
    ConfigError, DeserializeError, LookupError, MessageType, PhysicalStreamError, TermLocation,
};
use crate::lookup::{Lookup, LookupType};

use crate::proto::rdf_quad::Graph;
use crate::proto::rdf_stream_row::Row;
use crate::proto::{
    PhysicalStreamType, RdfDatatypeEntry, RdfIri, RdfLiteral, RdfNameEntry, RdfPrefixEntry,
    RdfQuad, RdfStreamFrame, RdfStreamOptions, RdfTriple, rdf_graph_start as gs, rdf_quad as q,
    rdf_triple as t,
};
use crate::to_rdf::{IriParams, LiteralParams, ToRdf};
use paste::paste;

// These functions are the same for all 6 types, I don't want to write that by hand
// Only difference is the q::Subject::SIri vs q::Predicate::PIri
pub trait ToTerm<Term, TermType> {
    fn to_term(&mut self, thing: Term) -> Result<TermType, DeserializeError>;
}

macro_rules! implTerm {
    ($k:path, $letter:ident, $($extra:tt)?) => {
        paste! {
                impl<T: ToRdf> ToTerm<$k, T::Term> for Deserializer<T> {
                    #[inline]
                    fn to_term(&mut self, thing: $k) -> Result<T::Term, DeserializeError> {
                        match thing {
                            $k::[<$letter Iri>](rdf_iri) => T::iri(rdf_iri, self.iri_params(&rdf_iri)?, self),
                            $k::[<$letter Bnode>](bnode) => T::bnode(bnode, self),
                            $k::[<$letter Literal>](rdf_literal) => {
                                let p = self.literal_params(&rdf_literal)?;
                                T::literal(rdf_literal, p, self)
                            },
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

pub trait RdfHandler<T: ToRdf> {
    fn handle_triple<'b>(&mut self, triple: T::Triple);
    fn handle_quad<'b>(&mut self, quad: T::Quad);
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
impl<'a, S, FT, FQ, T: ToRdf> RdfHandler<T> for StateHandler<S, FT, FQ>
where
    FT: FnMut(T::Triple, &mut S),
    FQ: FnMut(T::Quad, &mut S),
{
    fn handle_triple(&mut self, triple: <T as ToRdf>::Triple) {
        (self.ft)(triple, &mut self.state);
    }

    fn handle_quad(&mut self, quad: <T as ToRdf>::Quad) {
        (self.fq)(quad, &mut self.state);
    }
}

impl<'a, FT, FQ, T: ToRdf> RdfHandler<T> for (FT, FQ)
where
    FT: FnMut(T::Triple),
    FQ: FnMut(T::Quad),
{
    fn handle_triple(&mut self, triple: <T as ToRdf>::Triple) {
        self.0(triple)
    }

    fn handle_quad(&mut self, quad: <T as ToRdf>::Quad) {
        self.1(quad)
    }
}

impl<'a, FT, FQ, T: ToRdf> RdfHandler<T> for &mut (FT, FQ)
where
    FT: FnMut(T::Triple),
    FQ: FnMut(T::Quad),
{
    fn handle_triple(&mut self, triple: <T as ToRdf>::Triple) {
        self.0(triple)
    }

    fn handle_quad(&mut self, quad: <T as ToRdf>::Quad) {
        self.1(quad)
    }
}

pub struct Deserializer<T: ToRdf> {
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
impl<T: ToRdf> Deserializer<T>
where
    T::Term: Clone,
{
    pub fn spo(&self) -> Result<(T::Term, T::Term, T::Term), DeserializeError> {
        let s = self
            .last_subject
            .clone()
            .ok_or(DeserializeError::MissingTerm(TermLocation::Subject))?;
        let p = self
            .last_predicate
            .clone()
            .ok_or(DeserializeError::MissingTerm(TermLocation::Predicate))?;
        let o = self
            .last_object
            .clone()
            .ok_or(DeserializeError::MissingTerm(TermLocation::Object))?;
        Ok((s, p, o))
    }
}

impl<T: ToRdf> Deserializer<T> {
    pub fn new(t: T::State) -> Self {
        Self {
            name_table: Lookup::new(0),
            prefix_table: Lookup::new(0),
            datatype_table: Lookup::new(0),
            last_subject: None,
            last_predicate: None,
            last_object: None,
            last_graph: None,
            state: t,
            physical_type: PhysicalStreamType::Unspecified,
            graph_started: false,
        }
    }

    fn is_configured(&self) -> bool {
        self.physical_type != PhysicalStreamType::Unspecified
    }

    fn configure(&mut self, options: &RdfStreamOptions) -> Result<(), ConfigError> {
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

        self.name_table = Lookup::new(options.max_name_table_size);
        self.datatype_table = Lookup::new(options.max_datatype_table_size);
        self.prefix_table = Lookup::new(options.max_prefix_table_size);
        self.physical_type = physical_type;

        Ok(())
    }

    pub fn triple_term_terms(
        &mut self,
        triple: RdfTriple,
    ) -> Result<(T::Term, T::Term, T::Term), DeserializeError> {
        let RdfTriple {
            subject,
            predicate,
            object,
        } = triple;
        let s = if let Some(s) = subject {
            self.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermTermTriple(
                TermLocation::Subject,
            ));
        };

        let p = if let Some(s) = predicate {
            self.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermTermTriple(
                TermLocation::Predicate,
            ));
        };

        let o = if let Some(s) = object {
            self.to_term(s)?
        } else {
            return Err(DeserializeError::MissingTermTermTriple(
                TermLocation::Object,
            ));
        };
        Ok((s, p, o))
    }

    fn iri_params(&mut self, iri: &RdfIri) -> Result<IriParams, LookupError> {
        Ok(IriParams {
            name: self.name_table.get(iri.name_id, LookupType::Inc)?.clone(),
            prefix: self
                .prefix_table
                .get(iri.prefix_id, LookupType::Stay)?
                .clone(),
        })
    }

    fn literal_params(&mut self, literal: &RdfLiteral) -> Result<LiteralParams, LookupError> {
        let datatype = match literal.literal_kind {
            Some(crate::proto::rdf_literal::LiteralKind::Datatype(tag)) => {
                Some(self.datatype_table.get(tag, LookupType::Invalid)?.clone())
            }
            _ => None,
        };
        Ok(LiteralParams { datatype })
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
                if self.is_configured() {
                    info!("Didn't expect new options, but I don't care, ignoring");
                } else {
                    self.configure(options)?;
                    continue;
                }
            }

            if !self.is_configured() {
                return Err(DeserializeError::ConfigError(ConfigError::NotSet));
            }

            match row {
                Row::Options(_) => {}
                Row::Triple(rdf_triple) => {
                    if self.physical_type == PhysicalStreamType::Graphs {
                        if !self.graph_started {
                            return Err(DeserializeError::PhysicalStreamError(
                                PhysicalStreamError::NotYetSet {
                                    detected: self.physical_type,
                                    expected: MessageType::GraphStart,
                                },
                            ));
                        } else {
                            handler.handle_quad(self.triple_with_graph(rdf_triple)?);
                        }
                    } else if self.physical_type == PhysicalStreamType::Triples {
                        handler.handle_triple(self.triple(rdf_triple)?)
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: self.physical_type,
                                incoming: MessageType::Triple,
                            },
                        ));
                    }
                }
                Row::Quad(rdf_quad) => {
                    println!("Quad");
                    if self.physical_type == PhysicalStreamType::Quads {
                        handler.handle_quad(self.quad(rdf_quad)?)
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: self.physical_type,
                                incoming: MessageType::Quad,
                            },
                        ));
                    }
                }
                Row::GraphStart(rdf_graph_start) => {
                    println!("Graph start! {:?}", rdf_graph_start.graph);

                    if self.physical_type == PhysicalStreamType::Graphs {
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
                        self.q_graph(g)?;
                        self.graph_started = true;
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: self.physical_type,
                                incoming: MessageType::GraphStart,
                            },
                        ));
                    }
                }
                Row::GraphEnd(_) => {
                    if self.physical_type == PhysicalStreamType::Graphs {
                        println!("Graph end!");
                        self.last_graph = None;
                        self.graph_started = false;
                    } else {
                        return Err(DeserializeError::PhysicalStreamError(
                            PhysicalStreamError::IncorrectType {
                                detected: self.physical_type,
                                incoming: MessageType::GraphEnd,
                            },
                        ));
                    }
                }
                Row::Namespace(rdf_namespace_declaration) => {
                    info!("Name space is fine: {} ", rdf_namespace_declaration.name,);
                }
                Row::Name(rdf_name_entry) => self.name_entry(rdf_name_entry)?,
                Row::Prefix(rdf_prefix_entry) => self.prefix_entry(rdf_prefix_entry)?,
                Row::Datatype(rdf_datatype_entry) => self.datatype_entry(rdf_datatype_entry)?,
            }
        }
        Ok(handler)
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
            q::Graph::GIri(iri) => {
                self.last_graph = Some(T::iri(iri, self.iri_params(&iri)?, self)?)
            }
            q::Graph::GBnode(str) => self.last_graph = Some(T::bnode(str, self)?),
            q::Graph::GLiteral(literal) => {
                let p = self.literal_params(&literal)?;
                self.last_graph = Some(T::literal(literal, p, self)?)
            }
            q::Graph::GDefaultGraph(_) => self.last_graph = None,
        }
        Ok(())
    }

    #[inline]
    pub fn triple(&mut self, triple: RdfTriple) -> Result<T::Triple, DeserializeError> {
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
    pub fn triple_with_graph(&mut self, triple: RdfTriple) -> Result<T::Quad, DeserializeError> {
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
    pub fn quad(&mut self, quad: RdfQuad) -> Result<T::Quad, DeserializeError> {
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
