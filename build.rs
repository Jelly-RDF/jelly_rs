use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{self, PathBuf},
};

use sophia_api::{
    ns::rdf,
    parser::QuadParser,
    prelude::{Any, Dataset as _},
    source::QuadSource,
    term::Term,
};
use sophia_inmem::{dataset::GenericFastDataset, index::SimpleTermIndex};
use sophia_iri::Iri;

pub mod rdfs {
    use sophia_api::namespace;
    namespace! {
      "http://www.w3.org/2000/01/rdf-schema#",
      comment
    }
}

pub mod mf {
    use sophia_api::namespace;
    namespace! {
      "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#",
      entries, name, action, result
    }
}

pub mod jellyt {
    use sophia_api::namespace;
    namespace! {
      "https://w3id.org/jelly/dev/tests/vocab#",
      TestNegative, TestPositive, TestRdfFromJelly,requirementPhysicalTypeTriples, requirementPhysicalTypeQuads, requirementPhysicalTypeGraphs, requirementRdfStar, requirementGeneralizedRdf, featureNonDelimited
    }
}

pub mod rdft {
    use sophia_api::namespace;
    namespace! {
      "http://www.w3.org/ns/rdftest#",
      approval, Approved, TestTurtleEval, TestTurtlePositiveSyntax, TestTurtleNegativeSyntax
    }
}
#[allow(unused)]
#[derive(Debug)]
struct BasicInfo {
    ns: String,
    id: String,
    name: String,
    comment: String,
    approval: String,
    action: String,
    result: Option<Vec<String>>,
}

fn sanitize_test_name(original: &str) -> String {
    let mut s = String::new();
    // Prefix if first char is not a letter or underscore
    if !original
        .chars()
        .next()
        .map(|c| c.is_ascii_alphabetic() || c == '_')
        .unwrap_or(false)
    {
        s.push_str("case_");
    }
    let mut dashed = false;
    for c in original.chars() {
        if c.is_ascii_alphanumeric() {
            dashed = false;
            s.push(c.to_ascii_lowercase());
        } else {
            if !dashed {
                s.push('_');
                dashed = true;
            }
        }
    }
    if dashed {
        s.pop();
    }

    s
}

fn get_result<T: Term + Clone>(graph: &G, subj: T) -> Option<Vec<String>> {
    // let o = graph.
    let mut result = graph
        .quads_matching([subj.clone()], [mf::result], Any, Any)
        .next()?
        .ok()?
        .1[2];
    if let Some(iri) = result.iri() {
        if iri != rdf::nil.iriref() {
            return Some(vec![iri.to_string()]);
        }
    }

    let mut out = Vec::new();
    while result.iri() != Some(rdf::nil.iriref()) {
        let first = graph
            .quads_matching([result.clone()], [rdf::first], Any, Any)
            .next()?
            .ok()?
            .1[2]
            .iri()?
            .to_string();

        result = graph
            .quads_matching([result.clone()], [rdf::rest], Any, Any)
            .next()?
            .ok()?
            .1[2];
        out.push(first);
    }

    Some(out)
}
fn basic_info<T: Term + Clone>(graph: &G, subj: T) -> Option<BasicInfo> {
    let name = graph
        .quads_matching([subj.clone()], [mf::name], Any, Any)
        .next()?
        .ok()?
        .1[2]
        .lexical_form()?
        .to_string();

    let comment = graph
        .quads_matching([subj.clone()], [rdfs::comment], Any, Any)
        .next()
        .and_then(|x| Some(x.ok()?.1[2].lexical_form()?.to_string()))
        .unwrap_or_else(|| name.clone());

    let approval = graph
        .quads_matching([subj.clone()], [rdft::approval], Any, Any)
        .next()?
        .ok()?
        .1[2]
        .iri()?
        .to_string();

    let action = graph
        .quads_matching([subj.clone()], [mf::action], Any, Any)
        .next()?
        .ok()?
        .1[2]
        .iri()?
        .to_string();
    let subject = subj.iri()?.to_string();

    let result = get_result(graph, subj);

    let to_skip = "https://w3id.org/jelly/dev/tests/rdf/from_jelly/".len();
    let (ns, id) = subject[to_skip..]
        .split_once('/')
        .expect("two parts split by /");

    Some(BasicInfo {
        name: sanitize_test_name(&name),
        comment,
        approval,
        action,
        result,
        id: id.to_string(),
        ns: ns.to_string(),
    })
}

type G = GenericFastDataset<SimpleTermIndex<usize>>;
fn positive(graph: &G) -> Vec<BasicInfo> {
    let mut out = Vec::new();
    for (_, [subj, _, _]) in graph
        .quads_matching(Any, [rdf::type_], [jellyt::TestPositive], Any)
        .flatten()
    {
        match basic_info(graph, subj) {
            Some(info) => out.push(info),
            None => {
                println!("cargo:warning=Info failed for iri {:?}", subj);
            }
        }
    }
    out
}

fn negative(graph: &G) -> Vec<BasicInfo> {
    let mut out = Vec::new();
    for (_, [subj, _, _]) in graph
        .quads_matching(Any, [rdf::type_], [jellyt::TestNegative], Any)
        .flatten()
    {
        match basic_info(graph, subj) {
            Some(info) => out.push(info),
            None => {
                println!("cargo:warning=Info failed for iri {:?}", subj);
            }
        }
    }
    out
}

fn setup_from_jelly_tests() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let path = path::absolute("./proto/test/rdf/from_jelly/manifest.ttl").unwrap();
    let location = path.to_str().unwrap();

    println!("cargo:warning=Location {}", location);

    let tests_manifest = fs::read_to_string(&path).expect("Failed to read spec");

    let base = Iri::new(format!("file://{}", location)).unwrap();
    let parser = sophia_turtle::parser::gtrig::GTriGParser { base: Some(base) };

    let quads: G = parser
        .parse_str(&tests_manifest)
        .collect_quads()
        .expect("valid turtle");

    let mut generated = String::new();
    let positivies = positive(&quads);

    let mut nses = HashSet::new();
    let mut pos_map: HashMap<String, Vec<BasicInfo>> = HashMap::new();
    let mut neg_map: HashMap<String, Vec<BasicInfo>> = HashMap::new();

    for p in positivies {
        // You can just make this &str without the replace ...

        nses.insert(p.ns.clone());
        let entry = pos_map.entry(p.ns.clone()).or_default();
        entry.push(p);
    }

    for p in negative(&quads) {
        nses.insert(p.ns.clone());
        let entry = neg_map.entry(p.ns.clone()).or_default();
        entry.push(p);
    }

    for ns in nses {
        generated += &format!(r#" #[cfg(test)]mod {} {{"#, ns);

        if let Some(poses) = pos_map.get(&ns) {
            for p in poses {
                generated += &format!(
                    r#"
#[test]
            fn {}_{}() {{
                crate::init_logger();
                crate::test_positive({:?}, &{:?});
            }}
"#,
                    p.id,
                    p.name,
                    p.action,
                    p.result.as_ref().unwrap_or(&vec![])
                );
            }
        }

        if let Some(neges) = neg_map.get(&ns) {
            for p in neges {
                generated += &format!(
                    r#"
#[test]
            fn {}_{}() {{
                crate::init_logger();
                crate::test_negative({:?});
            }}
"#,
                    p.id, p.name, p.action,
                );
            }
        }

        generated += &format!(r#"}}"#);
    }

    fs::write(out_dir.join("generated_tests.rs"), generated)
        .expect("Failed to write generated tests");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_from_jelly_tests();
    prost_build::compile_protos(
        &[
            "proto/proto/rdf.proto",
            "proto/proto/patch.proto",
            "proto/proto/grpc.proto",
        ],
        &["proto/proto"],
    )?;
    Ok(())
}
