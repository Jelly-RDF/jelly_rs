use std::{
    collections::HashMap,
    fs::File,
    io::{Cursor, Read},
    ops::Deref,
};

use jelly::{
    FrameReader,
    deserialize::Deserializer,
    to_rdf::{StringRdf, ToRdf},
};

use jelly::{deserialize::StateHandler, to_rdf::SophiaRdf};
use sophia_inmem::{dataset::GenericFastDataset, index::SimpleTermIndex};

use sophia_api::{
    prelude::MutableDataset,
    quad::Quad,
    source::QuadSource,
    term::{SimpleTerm, Term},
    triple::Triple,
};

type T = SimpleTerm<'static>;
type Q = ([T; 3], Option<T>);

use std::sync::Once;

static INIT: Once = Once::new();

fn init_logger() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

/// Dataset comperator
struct DsCmp {
    state: HashMap<String, String>,
}

impl DsCmp {
    fn new() -> Self {
        Self {
            state: HashMap::new(),
        }
    }

    fn eq_term(&mut self, t1: &T, t2: &T) -> bool {
        if t1.kind() != t2.kind() {
            return false;
        }
        if t1.is_blank_node() && t2.is_blank_node() {
            let b1 = t1.bnode_id().unwrap();
            let b2 = t2.bnode_id().unwrap();

            if let Some(matched) = self.state.get(Deref::deref(&b1.borrowed())) {
                return matched.as_str() == b2.borrowed();
            } else {
                self.state
                    .insert(b1.borrowed().to_string(), b2.borrowed().to_string());
                return true;
            }
        }
        if t1.is_triple() && t2.is_triple() {
            return self.eq_triple(t1.triple().as_ref().unwrap(), t2.triple().as_ref().unwrap());
        }

        return t1 == t2;
    }

    fn eq_triple(&mut self, t1: &[&T; 3], t2: &[&T; 3]) -> bool {
        let s_match = self.eq_term(t1.s(), t2.s());
        let p_match = self.eq_term(t1.p(), t2.p());
        let o_match = self.eq_term(t1.o(), t2.o());

        return s_match && p_match && o_match;
    }

    fn eq_quad(&mut self, q1: &Q, q2: &Q) -> bool {
        let s_match = self.eq_term(q1.s(), q2.s());
        let p_match = self.eq_term(q1.p(), q2.p());
        let o_match = self.eq_term(q1.o(), q2.o());
        let q_match = match (q1.g(), q2.g()) {
            (Some(g1), Some(g2)) => self.eq_term(g1, g2),
            (None, None) => true,
            _ => false,
        };

        return s_match && p_match && o_match && q_match;
    }
    fn eq_ds(&mut self, ds1: &Vec<Q>, ds2: &Vec<Q>) -> bool {
        for (q1, q2) in ds1.iter().zip(ds2.iter()) {
            if !self.eq_quad(q1, q2) {
                println!("Q1 {:?}\nQ2 {:?}", q1, q2);
                return false;
            }
        }
        true
    }
}

const BASE: &'static str = "https://w3id.org/jelly/dev/tests";

fn read_manifested_file(input: &str) -> Vec<u8> {
    let input = input.replace(BASE, "./proto/test");
    let mut file = File::open(input).expect("file to exist");
    let mut content = Vec::new();
    file.read_to_end(&mut content).expect("read file");
    content
}

#[cfg(test)]
fn test_positive(input: &str, result: &[&str]) {
    use log::{debug, trace};

    let content = read_manifested_file(input);
    let frames = FrameReader::new(Cursor::new(content));

    trace!("test case with {:?} frame results", result);
    let mut result_iter = result.into_iter();
    let mut errored = false;
    let mut des = Deserializer::<SophiaRdf>::new();

    for frame in frames {
        let this_result = result_iter
            .next()
            .expect("equal amount of frames to results");
        debug!("Handling result file {}", this_result);

        let file = read_manifested_file(this_result);

        let graph_handler = StateHandler::new(
            Vec::new(),
            |triple: <SophiaRdf as ToRdf>::Triple<'_>, state: &mut Vec<Q>| {
                state
                    .insert_quad((triple, None))
                    .expect("triple inserted succesfully");
            },
            |quad: <SophiaRdf as ToRdf>::Quad<'_>, state: &mut Vec<Q>| {
                state.insert_quad(quad).expect("quad inserted succesfully");
            },
        );

        let expected_quads: Vec<Q> = sophia_turtle::parser::gnq::parse_bufread(Cursor::new(file))
            .collect_quads()
            .expect("valid nquads");

        let graph = match des.handle_frame(frame, graph_handler) {
            Err(e) => {
                println!("error {:?}", e);
                errored = true;
                break;
            }
            Ok(h) => h.state,
        };

        let mut state = DsCmp::new();

        let ds_eq = state.eq_ds(&expected_quads, &graph);
        assert!(ds_eq, "same same");
    }

    assert!(!errored, "positives tests should result in not error");
}

fn test_negative(input: &str) {
    let content = read_manifested_file(input);
    let frames = FrameReader::new(Cursor::new(content));

    let mut errored = false;
    let mut des = Deserializer::<StringRdf>::new();

    let mut h = (
        |_: <StringRdf as ToRdf>::Triple<'_>| {},
        |_: <StringRdf as ToRdf>::Quad<'_>| {},
    );

    for frame in frames {
        if let Err(_) = des.handle_frame(frame, &mut h) {
            errored = true;
            break;
        }
    }

    assert!(errored, "negative test should result in error");
}

include!(concat!(env!("OUT_DIR"), "/generated_tests.rs"));
