// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! `defer spec`: the offline OpenAPI/routes twin and typed client codegen.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Subcommand, ValueEnum};
use serde_json::json;

#[derive(clap::Args)]
pub(crate) struct SpecArgs {
    #[command(subcommand)]
    gen: Option<SpecSubcommand>,
    #[arg(long, value_enum, default_value_t = SpecFormat::Openapi)]
    format: SpecFormat,
}

#[derive(Subcommand)]
enum SpecSubcommand {
    Gen(GenArgs),
}

#[derive(Clone, Copy, ValueEnum)]
enum SpecFormat {
    Openapi,
    OpenapiYaml,
    Routes,
}

#[derive(clap::Args)]
struct GenArgs {
    #[arg(long, value_enum)]
    lang: GenLang,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, value_enum, default_value_t = GenHttp::Fetch)]
    http: GenHttp,
}

#[derive(Clone, Copy, ValueEnum)]
enum GenLang {
    Ts,
    Py,
    Rust,
}

#[derive(Clone, Copy, ValueEnum)]
enum GenHttp {
    Fetch,
    Axios,
}

// <HANDWRITE gap="missing-generator:logic" tracker="#2219" reason="Own the offline OpenAPI/routes projection and the route twin derived from the OpenAPI document, emitted from the Defer CLI.">
pub(crate) fn run(args: SpecArgs) -> Result<()> {
    let json = defer::openapi::openapi().to_pretty_json()?;
    if let Some(SpecSubcommand::Gen(args)) = args.gen {
        let lang = match args.lang {
            GenLang::Ts => openapi_codegen::Lang::Ts,
            GenLang::Py => openapi_codegen::Lang::Py,
            GenLang::Rust => openapi_codegen::Lang::Rust,
        };
        let output = openapi_codegen::generate(
            &json,
            &openapi_codegen::GenOptions::new(
                lang,
                PathBuf::new(),
                args.out.clone(),
                "createDeferClient",
            )
            .with_http_client(match args.http {
                GenHttp::Fetch => openapi_codegen::HttpClient::Fetch,
                GenHttp::Axios => openapi_codegen::HttpClient::Axios,
            }),
        )?;
        std::fs::create_dir_all(&args.out)?;
        for file in output.files {
            let path = args.out.join(file.rel_path);
            std::fs::write(&path, file.contents)?;
            println!("generated {}", path.display());
        }
        println!("next: done");
        return Ok(());
    }
    match args.format {
        SpecFormat::Openapi => println!("{json}"),
        SpecFormat::OpenapiYaml => {
            println!("{}", serde_yaml::to_string(&defer::openapi::openapi())?)
        }
        SpecFormat::Routes => println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "routes": defer::openapi::operations()
            }))?
        ),
    }
    println!("next: done");
    Ok(())
}
// </HANDWRITE>
// HANDWRITE-END
