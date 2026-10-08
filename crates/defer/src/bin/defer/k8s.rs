// HANDWRITE-BEGIN gap="missing-generator:logic:defer-cli" tracker="#766" reason="Agent-facing Defer CLI, shared conventions, service startup, and HTTP domain client."
//! `defer k8s`: render the CRD, the operator control plane, and Defer
//! instances offline; run the operator (feature `operator`).

use std::path::PathBuf;

use anyhow::Result;
use clap::{Subcommand, ValueEnum};

use crate::cli::write_or_print;

#[derive(clap::Args, Debug)]
pub(crate) struct K8sArgs {
    #[command(subcommand)]
    command: K8sCommand,
}

#[derive(Subcommand, Debug)]
enum K8sCommand {
    Crd(K8sCrdArgs),
    Operator(K8sOperatorArgs),
    Instance(K8sInstanceArgs),
}

#[derive(clap::Args, Debug)]
struct K8sCrdArgs {
    #[command(subcommand)]
    command: K8sCrdCommand,
}

#[derive(Subcommand, Debug)]
enum K8sCrdCommand {
    Render(OutputArgs),
}

#[derive(clap::Args, Debug)]
struct K8sOperatorArgs {
    #[command(subcommand)]
    command: Option<K8sOperatorCommand>,
}

#[derive(Subcommand, Debug)]
enum K8sOperatorCommand {
    Run,
    Render(OperatorRenderArgs),
}

#[derive(clap::Args, Debug)]
struct OperatorRenderArgs {
    #[arg(long, default_value = "defer-system")]
    namespace: String,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(clap::Args, Debug)]
struct K8sInstanceArgs {
    #[command(subcommand)]
    command: K8sInstanceCommand,
}

#[derive(Subcommand, Debug)]
enum K8sInstanceCommand {
    Render(InstanceRenderArgs),
}

#[derive(clap::Args, Debug)]
struct InstanceRenderArgs {
    #[arg(long, value_enum, default_value_t = InstanceProfile::Dev)]
    profile: InstanceProfile,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    namespace: Option<String>,
    #[arg(long)]
    image: Option<String>,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum InstanceProfile {
    Dev,
    Staging,
    Prod,
    Template,
}

#[derive(clap::Args, Debug)]
struct OutputArgs {
    #[arg(long)]
    out: Option<PathBuf>,
}

pub(crate) async fn run(args: K8sArgs) -> Result<()> {
    match args.command {
        K8sCommand::Crd(args) => match args.command {
            K8sCrdCommand::Render(args) => {
                write_or_print(args.out.as_deref(), "crd.yaml", &crd_yaml())
            }
        },
        K8sCommand::Operator(args) => match args.command.unwrap_or(K8sOperatorCommand::Run) {
            K8sOperatorCommand::Run => run_operator().await,
            K8sOperatorCommand::Render(args) => write_or_print(
                args.out.as_deref(),
                "operator.yaml",
                &operator_yaml(&args.namespace),
            ),
        },
        K8sCommand::Instance(args) => match args.command {
            K8sInstanceCommand::Render(args) => {
                write_or_print(args.out.as_deref(), "defer.yaml", &instance_yaml(&args))
            }
        },
    }
}

#[cfg(feature = "operator")]
async fn run_operator() -> Result<()> {
    defer::operator::run().await
}

#[cfg(not(feature = "operator"))]
async fn run_operator() -> Result<()> {
    anyhow::bail!("operator runtime requires a build with `--features operator`")
}

#[cfg(feature = "operator")]
fn crd_yaml() -> String {
    defer::operator::crd_yaml()
}

#[cfg(not(feature = "operator"))]
fn crd_yaml() -> String {
    cli_std::artifact::ensure_trailing_newline(include_str!("../../../../../k8s/operator/crd.yaml"))
}

fn operator_yaml(namespace: &str) -> String {
    let yaml = format!(
        "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {namespace}\n---\napiVersion: v1\nkind: ServiceAccount\nmetadata:\n  name: defer-operator\n  namespace: {namespace}\n---\napiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata:\n  name: defer-operator\nrules:\n  - apiGroups: [\"defer.dev\"]\n    resources: [\"defers\", \"defers/status\", \"defers/finalizers\"]\n    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n  - apiGroups: [\"apps\", \"batch\", \"policy\", \"coordination.k8s.io\"]\n    resources: [\"statefulsets\", \"cronjobs\", \"poddisruptionbudgets\", \"leases\"]\n    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n  - apiGroups: [\"\"]\n    resources: [\"services\", \"serviceaccounts\"]\n    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n---\napiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRoleBinding\nmetadata:\n  name: defer-operator\nroleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: ClusterRole\n  name: defer-operator\nsubjects:\n  - kind: ServiceAccount\n    name: defer-operator\n    namespace: {namespace}\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: defer-operator\n  namespace: {namespace}\nspec:\n  replicas: 2\n  selector:\n    matchLabels:\n      app.kubernetes.io/name: defer-operator\n  template:\n    metadata:\n      labels:\n        app.kubernetes.io/name: defer-operator\n    spec:\n      serviceAccountName: defer-operator\n      containers:\n        - name: operator\n          image: defer:{}\n          command: [\"defer\", \"k8s\", \"operator\", \"run\"]\n          env:\n            - name: POD_NAME\n              valueFrom: {{fieldRef: {{fieldPath: metadata.name}}}}\n            - name: POD_NAMESPACE\n              valueFrom: {{fieldRef: {{fieldPath: metadata.namespace}}}}\n          resources:\n            requests: {{cpu: 100m, memory: 128Mi}}\n            limits: {{cpu: 500m, memory: 256Mi}}\n          securityContext:\n            allowPrivilegeEscalation: false\n            readOnlyRootFilesystem: true\n            capabilities: {{drop: [\"ALL\"]}}\n",
        env!("CARGO_PKG_VERSION")
    );
    cli_std::artifact::ensure_trailing_newline(&yaml)
}

fn instance_yaml(args: &InstanceRenderArgs) -> String {
    let version = env!("CARGO_PKG_VERSION");
    let (namespace, image, body) = match args.profile {
        InstanceProfile::Dev => ("default", "defer:latest".to_string(), "  replicasPerShard: 1\n  voterCount: 1\n  storage: 1Gi\n  logLevel: debug\n"),
        InstanceProfile::Staging => ("staging", format!("defer:{version}"), "  replicasPerShard: 1\n  voterCount: 1\n  storage: 20Gi\n  logLevel: info\n"),
        InstanceProfile::Prod => ("production", format!("registry.example.com/defer:{version}"), "  imagePullPolicy: Always\n  replicasPerShard: 3\n  voterCount: 3\n  storage: 100Gi\n  graceSecs: 30\n  auth: required\n  tokensSecret: defer-token-registry\n  targetSigningSecret: defer-target-signing\n  targetSigningKeyId: active\n  peerTlsSecret: defer-peer-tls\n  backup:\n    schedule: \"0 */6 * * *\"\n    destination: s3://REPLACE_ME/defer\n    retentionSecs: 604800\n    adminTokenSecret: defer-backup-admin\n"),
        InstanceProfile::Template => ("REPLACE_ME__APP_NAMESPACE", "REPLACE_ME__REGISTRY/defer:REPLACE_ME__TAG".into(), "  replicasPerShard: REPLACE_ME__REPLICAS\n  voterCount: REPLACE_ME__VOTERS\n  storage: 10Gi\n"),
    };
    let name = args.name.as_deref().unwrap_or("defer");
    let namespace = args.namespace.as_deref().unwrap_or(namespace);
    let image = args.image.as_deref().unwrap_or(&image);
    cli_std::artifact::ensure_trailing_newline(&format!(
        "apiVersion: defer.dev/v1alpha1\nkind: Defer\nmetadata:\n  name: {name}\n  namespace: {namespace}\nspec:\n  image: {image}\n{body}  resources:\n    cpu: \"1\"\n    memory: 4Gi\n"
    ))
}
// HANDWRITE-END
