# jelly_js


Jelly_js brings WASM support to jelly_rs.

**jelly_rs** is a Rust implementation of [Jelly](https://w3id.org/jelly), a high-performance binary serialization format and streaming protocol for RDF knowledge graphs.

## Build locally 

Required: [wasm-pack](https://drager.github.io/wasm-pack/).

Compile:
```bash
wasm-pack build --target bundler
```
Bindings are available in `./pkg/`.


## Demo

With the demo, you can unpack Jelly files and peer into their contents (check the view triples checkbox).
It also supports remote Jelly files.

Local compile:
```bash
npm install
npm run serve
```

## Publish

This packages depends on `@rdfjs/types`, but `wasm-pack` currently provides no way to add required dependencies to the generated `package.json` ([issue](https://github.com/drager/wasm-pack/issues/1470).

So before publishing to npm add the dependency.
```bash
cd pkg
npm install @rdfjs/types
npm publish
```

