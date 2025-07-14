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

fn test_positive(input: &str, result: &[&str]) {
    let content = read_manifested_file(input);
    let frames = FrameReader::new(Cursor::new(content));

    let mut errored = false;
    let mut des = Deserializer::<StringRdf>::new();

    let mut h = (
        |_: <StringRdf as ToRdf>::Triple<'_>| {},
        |_: <StringRdf as ToRdf>::Quad<'_>| {},
    );

    for frame in frames {
        if let Err(e) = des.handle_frame(frame, &mut h) {
            println!("error {:?}", e);
            errored = true;
            break;
        }
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
