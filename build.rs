use std::{
    collections::HashSet,
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
#[derive(Debug)]
struct BasicInfo {
    subject: String,
    name: String,
    comment: String,
    approval: String,
    action: String,
    result: Option<Vec<String>>,
}

fn make_unique(sanitized: String, existing: &mut HashSet<String>) -> String {
    let mut name = sanitized.to_string();
    let mut counter = 1;
    while existing.contains(&name) {
        name = format!("{}_{}", sanitized, counter);
        counter += 1;
    }
    existing.insert(name.clone());
    name
}
fn sanitize_test_name(original: &str, existing: &mut HashSet<String>) -> String {
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

    make_unique(s, existing)
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
fn basic_info<T: Term + Clone>(
    graph: &G,
    subj: T,
    existing: &mut HashSet<String>,
) -> Option<BasicInfo> {
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

    Some(BasicInfo {
        name: sanitize_test_name(&name, existing),
        comment,
        approval,
        action,
        result,
        subject,
    })
}

type G = GenericFastDataset<SimpleTermIndex<usize>>;
fn positive(graph: &G, existing: &mut HashSet<String>) -> Vec<BasicInfo> {
    let mut out = Vec::new();
    for (_, [subj, _, _]) in graph
        .quads_matching(Any, [rdf::type_], [jellyt::TestPositive], Any)
        .flatten()
    {
        match basic_info(graph, subj, existing) {
            Some(info) => out.push(info),
            None => {
                println!("cargo:warning=Info failed for iri {:?}", subj);
            }
        }
    }
    out
}

fn negative(graph: &G, existing: &mut HashSet<String>) -> Vec<BasicInfo> {
    let mut out = Vec::new();
    for (_, [subj, _, _]) in graph
        .quads_matching(Any, [rdf::type_], [jellyt::TestNegative], Any)
        .flatten()
    {
        match basic_info(graph, subj, existing) {
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

    let mut existing = HashSet::new();
    let mut generated = String::from("\n #[cfg(test)]mod positive { \n ");
    let positivies = positive(&quads, &mut existing);

    for p in positivies {
        generated += &format!(
            r#"
#[test]
            fn {}() {{
                crate::test_positive({:?}, &{:?});
            }}
"#,
            p.name,
            p.action,
            p.result.unwrap_or(vec![])
        );
    }

    generated += "} #[cfg(test)]mod negative {";

    for p in negative(&quads, &mut existing) {
        generated += &format!(
            r#"
#[test]
            fn {}() {{
                crate::test_negative({:?});
            }}
"#,
            p.name, p.action,
        );
    }
    generated += "}";
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
