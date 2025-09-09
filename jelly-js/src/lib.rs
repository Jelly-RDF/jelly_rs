use futures::channel::mpsc::{self, UnboundedSender};
use jelly::Deserializer;
use jelly::deserialize::RdfHandler;
use jelly::error::{DeserializeError, LookupError};
use jelly::proto::rdf_literal::LiteralKind;
use jelly::to_rdf::{IriParams, LiteralParams, ToRdf};
use wasm_bindgen::prelude::*;

use wasm_bindgen::JsValue;
mod reader;

/// Represent the JS DataFactory with known fields
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "import('@rdfjs/types').DataFactory")]
    pub type DataFactory;

    pub type QuadConsumer;

    #[wasm_bindgen(method)]
    fn onQuad(this: &QuadConsumer, value: &Quad);

    #[wasm_bindgen(method)]
    fn namedNode(this: &DataFactory, value: &str) -> Term;

    #[wasm_bindgen(method)]
    fn blankNode(this: &DataFactory, value: &str) -> Term;

    #[wasm_bindgen(method, js_name = literal)]
    fn literal_simple(this: &DataFactory, value: &str) -> Term;

    // literal(value: string, languageOrDatatypeTag: string)
    #[wasm_bindgen(method, js_name = literal)]
    fn literal_lang(this: &DataFactory, value: &str, lang: &str) -> Term;

    // literal(value: string, languageOrDatatypeTag: Term)
    #[wasm_bindgen(method, js_name = literal)]
    fn literal_dt(this: &DataFactory, value: &str, datatype: &Term) -> Term;

    #[wasm_bindgen(method, js_name = quad)]
    fn triple(this: &DataFactory, s: &Term, p: &Term, o: &Term) -> Quad;

    #[wasm_bindgen(method)]
    fn quad(this: &DataFactory, s: &Term, p: &Term, o: &Term, g: &JsValue) -> Quad;

    #[wasm_bindgen(typescript_type = "import('@rdfjs/types').Term")]
    pub type Term; // Rust sees it as opaque, TS sees the real type

    #[wasm_bindgen(typescript_type = "import('@rdfjs/types').Quad")]
    pub type Quad; // Rust sees it as opaque, TS sees the real type
}

#[wasm_bindgen]
#[derive(Clone)]
pub struct ChunkSender(UnboundedSender<Vec<u8>>);

#[wasm_bindgen]
impl ChunkSender {
    pub fn send(&self, chunk: js_sys::Uint8Array) -> Result<(), String> {
        self.0
            .unbounded_send(chunk.to_vec())
            .map_err(|_| "unbounded send failed".to_string())?;
        Ok(())
    }
    pub fn close(&self) -> Result<(), String> {
        self.0.close_channel();
        Ok(())
    }
}

#[wasm_bindgen]
pub struct FrameReader {
    reader: jelly::FrameReader<reader::ChannelReader>,
    sender: ChunkSender,
}

#[wasm_bindgen]
impl FrameReader {
    #[wasm_bindgen(constructor)]
    pub fn new() -> FrameReader {
        console_error_panic_hook::set_once();
        let (tx, rx) = mpsc::unbounded();
        let reader = jelly::FrameReader::new(reader::ChannelReader::new(rx));
        let sender = ChunkSender(tx);

        Self { reader, sender }
    }

    pub fn sender(&self) -> ChunkSender {
        self.sender.clone()
    }
}

#[wasm_bindgen]
pub struct Handler {
    des: Deserializer<RdfJsRdf>,
    reader: FrameReader,
}

#[wasm_bindgen]
impl Handler {
    #[wasm_bindgen(constructor)]
    pub fn new(factory: DataFactory, reader: FrameReader) -> Handler {
        Handler {
            des: Deserializer::new(factory),
            reader,
        }
    }

    pub async fn next_frame(&mut self, handler: QuadConsumer) -> Result<bool, JsValue> {
        if let Some(frame) = self.reader.reader.next_frame().await {
            let handler = QCons(handler);
            self.des
                .handle_frame(frame, handler)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

struct QCons(QuadConsumer);
impl RdfHandler<RdfJsRdf> for QCons {
    fn handle_triple(&mut self, triple: <RdfJsRdf as ToRdf>::Triple) {
        self.0.onQuad(&triple);
    }

    fn handle_quad(&mut self, quad: <RdfJsRdf as ToRdf>::Quad) {
        self.0.onQuad(&quad);
    }
}

pub struct RdfJsRdf;
impl ToRdf for RdfJsRdf {
    type Term = Term;

    type Triple = Quad;

    type Quad = Quad;

    type State = DataFactory;

    fn default_term() -> Self::Term {
        todo!()
    }

    fn iri(
        _iri: jelly::proto::RdfIri,
        p: IriParams,
        deserializer: &mut jelly::Deserializer<Self>,
    ) -> Result<Self::Term, jelly::error::DeserializeError> {
        let iri = format!("{}{}", p.prefix, p.name);
        Ok(deserializer.state.namedNode(&iri))
    }

    fn bnode(
        key: String,
        deserializer: &mut jelly::Deserializer<Self>,
    ) -> Result<Self::Term, jelly::error::DeserializeError> {
        Ok(deserializer.state.blankNode(&key))
    }

    fn literal(
        literal: jelly::proto::RdfLiteral,
        p: LiteralParams,
        deserializer: &mut jelly::Deserializer<Self>,
    ) -> Result<Self::Term, jelly::error::DeserializeError> {
        let lex = literal.lex;
        Ok(match literal.literal_kind {
            Some(LiteralKind::Langtag(tag)) => deserializer.state.literal_lang(&lex, &tag),
            Some(LiteralKind::Datatype(_)) => {
                let dt = deserializer
                    .state
                    .namedNode(&p.datatype.ok_or(LookupError::InvalidLookupAction)?);
                deserializer.state.literal_dt(&lex, &dt)
            }
            None => deserializer.state.literal_simple(&lex),
        }
        .into())
    }

    fn term_triple(
        _triple: jelly::proto::RdfTriple,
        _deserializer: &mut jelly::Deserializer<Self>,
    ) -> Result<Self::Term, jelly::error::DeserializeError> {
        Err(DeserializeError::NotImplemented("triple terms for rdfjs"))
    }

    fn triple(
        deserializer: &mut jelly::Deserializer<Self>,
    ) -> Result<Self::Triple, jelly::error::DeserializeError> {
        let (s, p, o) = deserializer.spo_ref()?;
        Ok(deserializer.state.triple(s, p, o))
    }

    fn quad(
        deserializer: &mut jelly::Deserializer<Self>,
    ) -> Result<Self::Quad, jelly::error::DeserializeError> {
        let (s, p, o) = deserializer.spo_ref()?;
        if let Some(q) = deserializer.last_graph.as_ref() {
            Ok(deserializer.state.quad(s, p, o, q))
        } else {
            Ok(deserializer.state.triple(s, p, o))
        }
    }
}

#[wasm_bindgen]
#[wasm_bindgen]
extern "C" {
    pub fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet(name: &str) {
    alert(&format!("Hello, {}!", name));
}

/// Rust function now accepts a typed DataFactory
#[wasm_bindgen]
pub fn make_quad(factory: &DataFactory) -> Quad {
    let s = factory.namedNode("http://example.org/alice");
    let p = factory.namedNode("http://xmlns.com/foaf/0.1/knows");
    let o = factory.literal_lang("http://example.org/bob", "en");
    factory.quad(&s, &p, &o, &JsValue::undefined())
}
