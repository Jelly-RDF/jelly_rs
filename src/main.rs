#[macro_use]
extern crate log;

use std::{
    fs::File,
    io::{BufReader, Read},
};

use jelly::{
    Thing,
    proto::{RdfStreamFrame, rdf_stream_row::Row},
};
use prost::Message as _;

/// Read a Protobuf varint from an std::io::Read
fn read_varint<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;

    for _ in 0..10 {
        let mut byte = [0u8];
        if reader.read_exact(&mut byte).is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF during varint",
            ));
        }

        let b = byte[0];
        result |= ((b & 0x7F) as u64) << shift;

        if b & 0x80 == 0 {
            return Ok(result);
        }

        shift += 7;
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Varint too long",
    ))
}

struct Generator<R> {
    reader: BufReader<R>,
}

impl<R: Read> Iterator for Generator<R> {
    type Item = RdfStreamFrame;

    fn next(&mut self) -> Option<Self::Item> {
        // Decode a varint (length prefix)
        let len = match read_varint(&mut self.reader) {
            Ok(l) => l as usize,
            Err(_) => return None,
        };

        let mut buf = vec![0; len];

        // Read the exact number of bytes for the message
        self.reader.read_exact(&mut buf).ok()?;

        // Decode the message from the buffer
        let frame = RdfStreamFrame::decode(&*buf).ok()?;
        Some(frame)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init(); // Initialize logger, reads RUST_LOG env var
    let args: Vec<_> = std::env::args().into_iter().collect::<Vec<_>>();
    let file_name = args.get(1).expect("A file argument");
    let file = File::open(file_name)?;
    let generator = Generator {
        reader: BufReader::new(file),
    };

    let mut m_thing: Option<Thing> = None;

    for frame in generator {
        let rows = frame.rows.into_iter().flat_map(|x| x.row);

        for row in rows {
            debug!("Row {:?}", row);
            if let Row::Options(options) = &row {
                if m_thing.is_some() {
                    info!("Didn't expect new options, but I don't care, ignoring");
                } else {
                    m_thing = Some(Thing::from_options(&options));
                }
            }

            let thing = m_thing.as_mut().unwrap();
            match row {
                Row::Options(_) => {}
                Row::Triple(rdf_triple) => {
                    let (s, p, o) = thing.triple(rdf_triple);
                    println!("{} {} {} .", s, p, o);
                }
                Row::Quad(rdf_quad) => {
                    let (s, p, o, g) = thing.quad(rdf_quad);
                    if let Some(g) = g {
                        println!("{} {} {} {} .", s, p, o, g);
                    } else {
                        println!("{} {} {} .", s, p, o);
                    }
                }
                Row::GraphStart(rdf_graph_start) => todo!(),
                Row::GraphEnd(rdf_graph_end) => todo!(),
                Row::Namespace(rdf_namespace_declaration) => {
                    info!(
                        "Name space is fine: {} -> {}",
                        rdf_namespace_declaration.name,
                        rdf_namespace_declaration
                            .value
                            .map(|iri| thing.iri(iri))
                            .unwrap_or(String::new())
                    );
                }
                Row::Name(rdf_name_entry) => thing.name_entry(rdf_name_entry),
                Row::Prefix(rdf_prefix_entry) => thing.prefix_entry(rdf_prefix_entry),
                Row::Datatype(rdf_datatype_entry) => thing.datatype_entry(rdf_datatype_entry),
            }
        }
    }

    Ok(())
}
