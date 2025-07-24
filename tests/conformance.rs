use std::{
    fs::File,
    io::{Cursor, Read},
};

use jelly::{
    FrameReader,
    deserialize::Deserializer,
    to_rdf::{StringRdf, ToRdf},
};

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
    use jelly::{deserialize::StateHandler, to_rdf::SophiaRdf};
    use sophia_inmem::{dataset::GenericFastDataset, index::SimpleTermIndex};
    type G = GenericFastDataset<SimpleTermIndex<usize>>;

    use sophia_api::{prelude::MutableDataset, source::QuadSource};

    let content = read_manifested_file(input);
    let frames = FrameReader::new(Cursor::new(content));

    let mut result_iter = result.into_iter();
    let mut errored = false;
    let mut des = Deserializer::<SophiaRdf>::new();

    for frame in frames {
        let this_result = result_iter
            .next()
            .expect("equal amount of frames to results");
        let file = read_manifested_file(this_result);

        let graph_handler = StateHandler::new(
            G::new(),
            |triple: <SophiaRdf as ToRdf>::Triple<'_>, state: &mut G| {
                state
                    .insert_quad((triple, None))
                    .expect("triple inserted succesfully");
            },
            |quad: <SophiaRdf as ToRdf>::Quad<'_>, state: &mut G| {
                state.insert_quad(quad).expect("quad inserted succesfully");
            },
        );

        let expected_quads: G = sophia_turtle::parser::gnq::parse_bufread(Cursor::new(file))
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

        let mut expected_buf = Cursor::new(Vec::<u8>::new());

        sophia_c14n::rdfc10::normalize(&expected_quads, &mut expected_buf)
            .expect("expected canonical, works (expected)");

        let expected = unsafe { String::from_utf8_unchecked(expected_buf.into_inner()) };
        println!("expected\n{}", expected);

        let mut buf = Cursor::new(Vec::<u8>::new());
        sophia_c14n::rdfc10::normalize(&graph, &mut buf)
            .expect("expected canonical, works (graph)");
        let found = unsafe { String::from_utf8_unchecked(buf.into_inner()) };

        println!("found\n{}", found);
        assert_eq!(expected, found, "canonically same");
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
