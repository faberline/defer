#!/usr/bin/env python3
"""Static and fixture oracle for defer's candidate-first release contract.

defer releases as landed main -> immutable candidate -> digest-pinned GKE
acceptance -> protected annotated tag -> no-rebuild promotion. This oracle
pins the two workflows that carry it (`defer-release-candidate.yml`,
`defer-release.yml`), the gates the candidate runs, the PR CI that runs the
same gates, and the agreement between the three copies of the release table
(this file, scripts/release/apps.sh, scripts/release/make-gke-release-receipt.py).

The build-release skill, scripts/build/release.sh, and the gke-acceptance
workflow are not in this repository, so this copy does not pin them.

Usage:
  verify-release-contract.py               defer + standalone gates + tables
  verify-release-contract.py --app defer   the same checks, named
  verify-release-contract.py --self-test   the positive checks, static negative
                                           controls, and an offline release fixture

The checks never rewrite a checked file.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Dict, List, NoReturn, Optional, Sequence, Set, Tuple

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
RELEASE_DIR = ROOT / "scripts" / "release"
APPS_SH = RELEASE_DIR / "apps.sh"
RECEIPT_MAKER = RELEASE_DIR / "make-gke-release-receipt.py"
SHARED_CANDIDATE_VERIFIER = RELEASE_DIR / "verify-release-candidate.sh"
SHARED_ARTIFACT_VERIFIER = RELEASE_DIR / "verify-release-artifacts.sh"

REPO = "faberline/defer"
IMAGE_OWNER = "ghcr.io/faberline"
ACCEPTANCE_JOB = "deploy + verify on GKE"
VERIFY_IMAGE_STEP = "Verify prebuilt image input"
HARNESS_STEP = "Run acceptance harness"
PARK_STEP = "Park node pool (belt and suspenders)"
FIXTURE_BANNER = "LOCAL FIXTURE ONLY"

TWO_TARGETS = ("x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl")
HARNESS_FIELDS = ("readyz", "round_trip", "durability")


@dataclass(frozen=True)
class App:
    name: str
    root: str
    scripts_dir: str
    shared_scripts: bool
    onboarded: bool
    targets: Tuple[str, ...]
    candidate_jobs: Tuple[str, ...]
    promotion_prefix: str
    takes_attempt: bool
    manifest_schema: str
    receipt_name: str
    receipt_schema: str
    gke_backend: str
    functional: Tuple[str, ...]

    @property
    def promotion_inputs(self) -> Tuple[str, ...]:
        base = ["version", "candidate_run_id"]
        if self.takes_attempt:
            base.append("candidate_run_attempt")
        prefix = self.promotion_prefix
        return tuple(base + [f"{prefix}_b64", f"{prefix}_sha256", f"{prefix}_sidecar_b64", f"{prefix}_sidecar_sha256"])

    @property
    def candidate_workflow(self) -> Path:
        return WORKFLOWS / f"{self.name}-release-candidate.yml"

    @property
    def promotion_workflow(self) -> Path:
        return WORKFLOWS / f"{self.name}-release.yml"


def shared_jobs(app: str) -> Tuple[str, ...]:
    return ("identity", "build", f"{app}-release-gates", "ghcr-image-and-attest", "manifest", "verify-candidate", "result")


def shared_app(name: str, root: str, targets: Tuple[str, ...], backend: str, functional: Tuple[str, ...], onboarded: bool) -> App:
    return App(
        name=name,
        root=root,
        scripts_dir="scripts/release",
        shared_scripts=True,
        onboarded=onboarded,
        targets=targets,
        candidate_jobs=shared_jobs(name),
        promotion_prefix="gke_receipt",
        takes_attempt=True,
        manifest_schema=f"cclab.{name}.candidate-manifest.v1",
        receipt_name=f"{name}-gke-receipt.json",
        receipt_schema=f"{name}.gke-release-receipt/v1",
        gke_backend=backend,
        functional=functional,
    )


APPS: Dict[str, App] = {
    "defer": shared_app("defer", ".", TWO_TARGETS, "gke-acceptance", HARNESS_FIELDS, onboarded=True),
}
FIXTURE_APPS = ("defer",)
FIXTURE_VERSION = {"defer": "0.4.13"}
CI_WORKFLOW = WORKFLOWS / "defer-ci.yml"
# Every gate the candidate runs before it builds anything, and PR CI with it.
STANDALONE_GATES = (
    "cargo fmt --all -- --check",
    "cargo clippy --locked --workspace --all-targets --all-features -- -D warnings",
    "cargo test --locked --workspace",
    "cargo test --locked -p defer --features operator --test operator",
    "python3 -B scripts/meta/project_docs_contract.py check .",
)


# --- helpers -----------------------------------------------------------------


class ContractError(Exception):
    pass


def fail(message: str) -> NoReturn:
    raise ContractError(message)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path: Path) -> str:
    if not path.is_file():
        fail(f"missing file: {path.relative_to(ROOT)}")
    return path.read_text(encoding="utf-8")


def top_block(text: str, key: str) -> str:
    lines = text.splitlines()
    try:
        start = lines.index(f"{key}:")
    except ValueError:
        fail(f"top-level key {key!r} is absent")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line and not line.startswith(" ") and not line.startswith("#"):
            end = index
            break
    return "\n".join(lines[start:end])


def workflow_triggers(text: str) -> List[str]:
    return re.findall(r"^  ([a-z_]+):$", top_block(text, "on"), re.M)


def workflow_inputs(text: str) -> set:
    return set(re.findall(r"^      ([a-z][a-z0-9_]*):$", top_block(text, "on"), re.M))


def job_ids(text: str) -> set:
    return set(re.findall(r"^  ([a-z][a-z0-9-]*):$", top_block(text, "jobs"), re.M))


def job_block(text: str, job: str) -> str:
    jobs = top_block(text, "jobs")
    lines = jobs.splitlines()
    try:
        start = lines.index(f"  {job}:")
    except ValueError:
        fail(f"job {job} is absent")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if re.match(r"^  [a-z][a-z0-9-]*:$", lines[index]):
            end = index
            break
    return "\n".join(lines[start:end])


def step_block(job_text: str, step_name: str) -> str:
    lines = job_text.splitlines()
    marker = f"- name: {step_name}"
    starts = [index for index, line in enumerate(lines) if line.strip() == marker]
    if len(starts) != 1:
        fail(f"step {step_name!r} must appear exactly once, found {len(starts)}")
    start = starts[0]
    indent = len(lines[start]) - len(lines[start].lstrip())
    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line.strip() and (len(line) - len(line.lstrip())) <= indent and line.lstrip().startswith("- "):
            end = index
            break
    return "\n".join(lines[start:end])


def run_script(step_text: str) -> str:
    lines = step_text.splitlines()
    starts = [index for index, line in enumerate(lines) if line.strip() in ("run: |", "run: |-")]
    if len(starts) != 1:
        fail("step must carry exactly one block run script")
    start = starts[0]
    indent = len(lines[start]) - len(lines[start].lstrip())
    body = []
    for line in lines[start + 1:]:
        if line.strip() and (len(line) - len(line.lstrip())) <= indent:
            break
        body.append(line)
    return "\n".join(body)


def all_run_scripts(text: str) -> List[str]:
    scripts: List[str] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped in ("run: |", "run: |-"):
            indent = len(line) - len(line.lstrip())
            body = []
            for following in lines[index + 1:]:
                if following.strip() and (len(following) - len(following.lstrip())) <= indent:
                    break
                body.append(following)
            scripts.append("\n".join(body))
        elif stripped.startswith("run: ") and stripped not in ("run: |", "run: |-"):
            scripts.append(stripped[len("run: "):])
    return scripts


def action_values(text: str) -> List[str]:
    return re.findall(r"^\s*(?:-\s+)?uses:\s*(\S+)", text, re.M)


def assert_actions_pinned(label: str, text: str) -> None:
    for value in action_values(text):
        if value.startswith("./"):
            continue
        if "@" not in value or not re.fullmatch(r"[0-9a-f]{40}", value.rsplit("@", 1)[1]):
            fail(f"{label}: action not pinned to a 40-hex commit: {value}")


def shell_tokens(script: str) -> List[str]:
    tokens: List[str] = []
    for raw in script.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            tokens.extend(shlex.split(line, comments=True))
        except ValueError:
            tokens.extend(line.split())
    return tokens


def assert_no_rebuild(label: str, text: str) -> None:
    forbidden = (("docker", "build"), ("docker", "buildx", "build"), ("cargo", "build"), ("cargo", "zigbuild"), ("cross", "build"))
    for script in all_run_scripts(text):
        tokens = shell_tokens(script)
        for start in range(len(tokens)):
            for pattern in forbidden:
                if tuple(tokens[start:start + len(pattern)]) == pattern:
                    fail(f"{label}: promotion must not rebuild: {' '.join(pattern)}")


def parse_needs(job_text: str) -> List[str]:
    match = re.search(r"^\s*needs: \[([^\]]+)\]$", job_text, re.M)
    if not match:
        fail("result job must declare a bracketed needs list")
    return [item.strip() for item in match.group(1).split(",")]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if text.count(old) != 1:
        fail(f"{label}: mutation anchor must appear exactly once: {old!r} ({text.count(old)})")
    return text.replace(old, new)


def replace_all(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        fail(f"{label}: mutation anchor is absent: {old!r}")
    return text.replace(old, new)


def unpin_first_action(text: str, label: str) -> str:
    mutated, count = re.subn(r"(uses: [^@\s]+)@[0-9a-f]{40}", r"\1@v4", text, count=1)
    if count != 1:
        fail(f"{label}: no pinned action to unpin")
    return mutated


def insert_after_first_run_block(text: str, line: str, label: str) -> str:
    match = re.search(r"^(\s*)run: \|-?$", text, re.M)
    if not match:
        fail(f"{label}: no block run script to mutate")
    indent = " " * (len(match.group(1)) + 2)
    return text[: match.end()] + "\n" + indent + line + text[match.end():]


def expect_static_failure(label: str, action: Callable[[], None]) -> None:
    try:
        action()
    except ContractError as exc:
        print(f"negative control refused: {label}: {exc}")
        return
    fail(f"negative control unexpectedly passed: {label}")


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_sidecar(path: Path) -> Path:
    sidecar = path.with_name(path.name + ".sha256")
    sidecar.write_text(f"{sha256(path)}  {path.name}\n", encoding="utf-8")
    return sidecar


def make_archive(path: Path, app: str, target: str, version: str) -> None:
    top = f"{app}-{target}"
    binary = f"#!/bin/sh\nprintf '%s %s\\n' '{app}' '{version}'\n".encode()
    readme = f"{app} release fixture for {target}\n".encode()
    with tarfile.open(path, "w:gz") as tar:
        directory = tarfile.TarInfo(top)
        directory.type = tarfile.DIRTYPE
        directory.mode = 0o755
        tar.addfile(directory)
        info = tarfile.TarInfo(f"{top}/README.md")
        info.size = len(readme)
        info.mode = 0o644
        tar.addfile(info, io.BytesIO(readme))
        info = tarfile.TarInfo(f"{top}/{app}")
        info.size = len(binary)
        info.mode = 0o755
        tar.addfile(info, io.BytesIO(binary))


def run_command(cmd: Sequence[str], *, label: str, expect_ok: bool, must_print: Optional[str] = None) -> None:
    proc = subprocess.run(list(cmd), capture_output=True, text=True, cwd=str(ROOT))
    if expect_ok:
        if proc.returncode != 0:
            fail(f"{label}: exit {proc.returncode}\n{proc.stdout}{proc.stderr}")
        if must_print and must_print not in proc.stdout:
            fail(f"{label}: expected {must_print!r} on stdout, got:\n{proc.stdout}")
        return
    if proc.returncode == 0:
        fail(f"negative control unexpectedly passed: {label}\n{proc.stdout}")
    last = (proc.stderr.strip().splitlines() or proc.stdout.strip().splitlines() or ["<no output>"])[-1]
    print(f"negative control refused: {label}: {last}")


# --- static checks: candidate and promotion workflows -----------------------


TRIPLE = re.compile(r"^[a-z0-9_]+-(?:unknown|apple|pc)-[a-z]+(?:-[a-z0-9]+)?$")


def referenced_targets(app: App, text: str) -> Set[str]:
    """Rust triples named by `<app>-candidate-<triple>-${{ github.run_id }}` artifact references."""
    names = re.findall(rf"{re.escape(app.name)}-candidate-([A-Za-z0-9_-]+?)-\$\{{\{{ github\.run_id \}}\}}", text)
    return {name for name in names if TRIPLE.match(name)}


def check_candidate_targets(app: App, text: str) -> None:
    """Every target-bearing surface of the candidate workflow names exactly the
    apps.sh target set: the build matrix, each job that downloads per-target
    archives, and each `for target in` loop. A matrix trimmed without its
    download steps passes YAML and this checker's job-set test, then fails only
    at run time with "Artifact not found" (keep run 33903311302)."""
    label = f"{app.name} candidate"
    expected = set(app.targets)
    matrix = set(re.findall(r"^\s*- \{ target: ([A-Za-z0-9_-]+),", text, re.MULTILINE))
    if matrix != expected:
        fail(f"{label}: build matrix targets {sorted(matrix)} differ from apps.sh {sorted(expected)}")
    for job in ("manifest", "verify-candidate", "result"):
        seen = referenced_targets(app, job_block(text, job))
        if seen and seen != expected:
            fail(f"{label}: job {job} downloads targets {sorted(seen)}, apps.sh says {sorted(expected)}")
    loops = re.findall(r"for target in ([A-Za-z0-9_ -]+); do", text)
    if not loops:
        fail(f"{label}: no `for target in ...; do` loop binds the archives into the manifest")
    for loop in loops:
        if set(loop.split()) != expected:
            fail(f"{label}: loop targets {loop.split()} differ from apps.sh {sorted(expected)}")


def drop_matrix_row(app: App, text: str, label: str) -> str:
    pattern = rf"^\s*- \{{ target: {re.escape(app.targets[0])},[^\n]*\n"
    if not re.search(pattern, text, re.MULTILINE):
        fail(f"{label}: control could not find the {app.targets[0]} matrix row")
    return re.sub(pattern, "", text, count=1, flags=re.MULTILINE)


def check_candidate(app: App, text: str) -> None:
    label = f"{app.name} candidate"
    if workflow_triggers(text) != ["workflow_dispatch"]:
        fail(f"{label}: the only trigger must be workflow_dispatch, got {workflow_triggers(text)}")
    if workflow_inputs(text) != {"version", "commit"}:
        fail(f"{label}: inputs must be exactly version and commit, got {sorted(workflow_inputs(text))}")
    if job_ids(text) != set(app.candidate_jobs):
        fail(f"{label}: jobs must be {sorted(app.candidate_jobs)}, got {sorted(job_ids(text))}")
    assert_actions_pinned(label, text)
    for required in (
        f"group: {app.name}-release-candidate-${{{{ inputs.version }}}}",
        app.manifest_schema,
        f"{IMAGE_OWNER}/{app.name}",
        f"refs/tags/{app.name}@",
        '[[ "$GITHUB_REF" == "refs/heads/main" ]]',
        '[[ "$REQUESTED_COMMIT" == "$GITHUB_SHA" ]]',
        f".github/workflows/{app.name}-release-candidate.yml@refs/heads/main",
    ):
        if required not in text:
            fail(f"{label}: required fragment absent: {required!r}")
    verify = job_block(text, "verify-candidate")
    verifier = f"{app.scripts_dir}/verify-release-candidate.sh"
    if verifier not in verify:
        fail(f"{label}: verify-candidate must run {verifier}")
    if "--mode full" not in verify:
        fail(f"{label}: verify-candidate must run the verifier in full mode")
    if app.shared_scripts and f"--app {app.name}" not in verify:
        fail(f"{label}: the shared verifier must be told --app {app.name}")
    result = job_block(text, "result")
    needs = parse_needs(result)
    expected_needs = set(app.candidate_jobs) - {"result"}
    if len(needs) != len(set(needs)) or set(needs) != expected_needs:
        fail(f"{label}: result must need exactly {sorted(expected_needs)}, got {needs}")
    for job in app.candidate_jobs:
        if job == "result":
            pattern = r'(?:"result"|result):"success"'
        else:
            pattern = rf'(?:"{re.escape(job)}"|{re.escape(job)}):"\$\{{\{{ needs\.{re.escape(job)}\.result \}}\}}"'
        if not re.search(pattern, result):
            fail(f"{label}: the final manifest must bind job {job} to its own result")
    check_candidate_targets(app, text)


def check_promotion(app: App, text: str) -> None:
    label = f"{app.name} promotion"
    if workflow_triggers(text) != ["workflow_dispatch"]:
        fail(f"{label}: the only trigger must be workflow_dispatch, got {workflow_triggers(text)}")
    if workflow_inputs(text) != set(app.promotion_inputs):
        fail(f"{label}: inputs must be {sorted(app.promotion_inputs)}, got {sorted(workflow_inputs(text))}")
    if job_ids(text) != {"verify-inputs", "publish-release"}:
        fail(f"{label}: jobs must be verify-inputs and publish-release, got {sorted(job_ids(text))}")
    assert_actions_pinned(label, text)
    assert_no_rebuild(label, text)
    for required in (
        f"group: {app.name}-release-promotion-${{{{ inputs.version }}}}",
        f"refs/tags/{app.name}@${{{{ inputs.version }}}}",
        app.receipt_name,
        f"{IMAGE_OWNER}/{app.name}",
        f"{app.scripts_dir}/verify-release-artifacts.sh",
        "--mode candidate",
        "--mode public",
    ):
        if required not in text:
            fail(f"{label}: required fragment absent: {required!r}")
    if app.shared_scripts:
        if f"--app {app.name}" not in text:
            fail(f"{label}: the shared artifact verifier must be told --app {app.name}")
        # The shared public verifier refuses release notes without exactly one
        # RELEASE_COMPATIBILITY_LINE; catch that here, before a promotion run.
        compatibility = bash_variable("RELEASE_COMPATIBILITY_LINE")
        notes = [line for line in text.splitlines() if line.strip() == compatibility]
        if len(notes) != 1:
            fail(f"{label}: release notes must carry apps.sh RELEASE_COMPATIBILITY_LINE exactly once, got {len(notes)}")
    if app.takes_attempt and "--candidate-run-attempt" not in text:
        fail(f"{label}: the artifact verifier must receive the candidate run attempt")
    creates = [line.strip() for line in text.splitlines() if "docker buildx imagetools create" in line]
    if len(creates) != 2:
        fail(f"{label}: expected exactly two imagetools create lines (semver, latest), got {len(creates)}")
    for line in creates:
        if not line.endswith('"$root_ref"'):
            fail(f"{label}: every retag must source the candidate root digest: {line}")


def check_app(app: App) -> None:
    if not app.onboarded:
        fail(f"{app.name} is not onboarded yet: its release workflows do not exist (flip onboarded when they land)")
    check_candidate(app, read(app.candidate_workflow))
    check_promotion(app, read(app.promotion_workflow))


def check_not_onboarded(app: App) -> None:
    # Only the candidate workflow proves onboarding: a `<app>-release.yml` may
    # predate onboarding (sift kept the retired tag-first route until its rewrite).
    path = app.candidate_workflow
    if path.exists():
        fail(f"{path.relative_to(ROOT)} exists but {app.name} is still marked onboarded=False here")


# --- cross-checks: apps.sh and the receipt maker -----------------------------


def bash_values(function: str, app: str) -> List[str]:
    proc = subprocess.run(
        ["bash", "-c", 'source "$1"; shift; "$@"', "_", str(APPS_SH), function, app],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        fail(f"apps.sh {function} {app}: exit {proc.returncode}: {proc.stderr.strip()}")
    return [line for line in proc.stdout.split("\n") if line]


def bash_predicate(function: str, app: str) -> bool:
    proc = subprocess.run(
        ["bash", "-c", 'source "$1"; shift; "$@"', "_", str(APPS_SH), function, app],
        capture_output=True,
        text=True,
    )
    if proc.returncode not in (0, 1):
        fail(f"apps.sh {function} {app}: exit {proc.returncode}: {proc.stderr.strip()}")
    return proc.returncode == 0


def bash_variable(name: str) -> str:
    proc = subprocess.run(
        ["bash", "-c", 'source "$1"; printf "%s" "${!2}"', "_", str(APPS_SH), name],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0 or not proc.stdout:
        fail(f"apps.sh does not define {name}: exit {proc.returncode}: {proc.stderr.strip()}")
    return proc.stdout


def load_receipt_maker():
    spec = importlib.util.spec_from_file_location("make_gke_release_receipt", RECEIPT_MAKER)
    if spec is None or spec.loader is None:
        fail(f"cannot load {RECEIPT_MAKER.relative_to(ROOT)}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_tables(maker) -> None:
    for app in APPS.values():
        def same(function: str, expected: Sequence[str]) -> None:
            actual = bash_values(function, app.name)
            if list(actual) != list(expected):
                fail(f"apps.sh {function} {app.name} = {actual}, this table says {list(expected)}")

        same("release_app_root", [app.root])
        same("release_app_scripts_dir", [app.scripts_dir])
        same("release_app_targets", app.targets)
        same("release_app_gke_backend", [app.gke_backend])
        same("release_app_receipt_name", [app.receipt_name])
        same("release_app_receipt_schema", [app.receipt_schema])
        same("release_app_promotion_input_prefix", [app.promotion_prefix])
        same("release_app_manifest_schema", [app.manifest_schema])
        same("release_app_candidate_jobs", app.candidate_jobs)
        if bash_predicate("release_app_uses_shared_scripts", app.name) != app.shared_scripts:
            fail(f"apps.sh disagrees on whether {app.name} uses the shared scripts")
        if bash_predicate("release_app_promotion_takes_attempt", app.name) != app.takes_attempt:
            fail(f"apps.sh disagrees on whether {app.name} promotion takes a run attempt")
        if app.shared_scripts:
            same("release_app_functional_fields", app.functional)
    if bash_predicate("release_app_known", "loom"):
        fail("apps.sh must not know loom: it has no candidate or promotion")

    shared = {name for name, app in APPS.items() if app.shared_scripts}
    if set(maker.APPS) != shared:
        fail(f"receipt maker knows {sorted(maker.APPS)}, shared-script apps are {sorted(shared)}")
    for name in shared:
        app = APPS[name]
        entry = maker.APPS[name]
        if tuple(entry["targets"]) != app.targets:
            fail(f"receipt maker targets for {name} differ from this table")
        if entry["backend"] != app.gke_backend:
            fail(f"receipt maker backend for {name} is {entry['backend']}, table says {app.gke_backend}")
        if tuple(entry["functional"]) != app.functional:
            fail(f"receipt maker functional fields for {name} differ from this table")
        if maker.candidate_schema(name) != app.manifest_schema:
            fail(f"receipt maker candidate schema for {name} differs from this table")
        if maker.receipt_schema(name) != app.receipt_schema:
            fail(f"receipt maker receipt schema for {name} differs from this table")
        if tuple(maker.final_jobs(name)) != app.candidate_jobs:
            fail(f"receipt maker job inventory for {name} differs from this table")
    for attribute, expected in (
        ("REPOSITORY", REPO),
        ("IMAGE_OWNER", IMAGE_OWNER),
        ("ACCEPTANCE_JOB", ACCEPTANCE_JOB),
        ("VERIFY_IMAGE_STEP", VERIFY_IMAGE_STEP),
        ("HARNESS_STEP", HARNESS_STEP),
        ("PARK_STEP", PARK_STEP),
    ):
        if getattr(maker, attribute, None) != expected:
            fail(f"receipt maker {attribute} = {getattr(maker, attribute, None)!r}, expected {expected!r}")


# --- standalone gates: the candidate and PR CI ---------------------------------


def gate_lines(script: str) -> List[str]:
    return [line for line in logical_lines(script) if line]


def check_standalone_gates(candidate: str, ci: str) -> None:
    gates = gate_lines(run_script(step_block(job_block(candidate, "defer-release-gates"), "Run declared Defer gates")))
    for gate in STANDALONE_GATES + ("uv run --python 3.13 --no-project scripts/release/verify-release-contract.py --app defer",):
        if gate not in gates:
            fail(f"defer candidate: release gates must run {gate!r}")
    label = "defer-ci"
    if workflow_triggers(ci) != ["pull_request", "push"]:
        fail(f"{label}: triggers must be pull_request and push, got {workflow_triggers(ci)}")
    if "    branches: [main]" not in top_block(ci, "on"):
        fail(f"{label}: push must be limited to main")
    assert_actions_pinned(label, ci)
    ci_gates = [line for script in all_run_scripts(ci) for line in gate_lines(script)]
    for gate in STANDALONE_GATES:
        if gate not in ci_gates:
            fail(f"{label}: PR CI must run {gate!r}")
    if "permissions:\n  contents: read" not in ci:
        fail(f"{label}: PR CI must run with read-only contents permission")


# --- shell parsing -------------------------------------------------------------


def logical_lines(text: str) -> List[str]:
    """Non-comment lines with backslash continuations joined, as a shell reads them."""
    logical: List[str] = []
    current = ""
    for raw in text.split("\n"):
        trimmed = raw.strip()
        if trimmed.startswith("#"):
            continue
        if trimmed.endswith("\\"):
            current += trimmed[:-1].rstrip() + " "
            continue
        current += trimmed
        if current.strip():
            logical.append(current.strip())
        current = ""
    if current.strip():
        logical.append(current.strip())
    return logical


# --- offline release fixture -------------------------------------------------


@dataclass
class Fixture:
    app: App
    version: str
    commit: str
    run_id: str
    attempt: str
    candidate_dir: Path
    release_dir: Path
    manifest: Path
    receipt: Path
    sidecar: Path


def build_fixture(app: App, root: Path, maker) -> Fixture:
    version = FIXTURE_VERSION[app.name]
    commit = "f" * 40
    run_id, attempt = "424242", "2"
    candidate_dir = root / "candidate"
    release_dir = root / "release"
    candidate_dir.mkdir()
    release_dir.mkdir()
    manifest = maker.fixture_candidate(app.name, version, commit, run_id, attempt)
    artifacts = []
    for target in app.targets:
        archive = candidate_dir / f"{app.name}-{target}.tar.gz"
        make_archive(archive, app.name, target, version)
        sidecar = write_sidecar(archive)
        artifacts.append({
            "target": target,
            "archive": archive.name,
            "archive_sha256": sha256(archive),
            "sidecar": sidecar.name,
            "sidecar_sha256": sha256(sidecar),
        })
    manifest["artifacts"] = artifacts
    for arch in ("amd64", "arm64"):
        spdx = candidate_dir / f"spdx-{arch}.json"
        write_json(spdx, {"spdxVersion": "SPDX-2.3", "name": f"{app.name}-{arch}", "packages": []})
        manifest["sboms"][arch] = {"file": spdx.name, "sha256": sha256(spdx)}
    manifest_path = candidate_dir / "final-candidate-manifest.json"
    write_json(manifest_path, manifest)
    write_sidecar(manifest_path)

    image = f"{IMAGE_OWNER}/{app.name}@{manifest['image']['root_digest']}"
    receipt = root / app.receipt_name
    evidence = root / "evidence"
    evidence.mkdir()
    try:
        if app.gke_backend == "gke-acceptance":
            gh_run = evidence / "gh-run.json"
            write_json(gh_run, maker.fixture_gke_acceptance(app.name, commit, image))
            bundle = evidence / "bundle"
            maker.write_harness_evidence(app.name, bundle, image)
            maker.make_receipt(app.name, "gke-acceptance", manifest_path, receipt, gh_run=gh_run, evidence_dir=bundle)
        elif app.gke_backend == "gcp":
            inputs = maker.fixture_gcp(app.name, commit, image)
            paths = {}
            for key, value in inputs.items():
                paths[key] = evidence / f"{key}.json"
                write_json(paths[key], value)
            maker.make_receipt(app.name, "gcp", manifest_path, receipt, **paths)
        else:
            fail(f"no fixture for backend {app.gke_backend}")
    except SystemExit as exc:
        fail(f"{app.name} receipt maker refused the fixture: {exc}")
    sidecar = receipt.with_name(receipt.name + ".sha256")
    if not sidecar.is_file():
        sidecar = write_sidecar(receipt)
    for path in sorted(candidate_dir.iterdir()):
        if path.name.startswith("final-candidate-manifest"):
            continue
        shutil.copy2(path, release_dir / path.name)
    shutil.copy2(receipt, release_dir / receipt.name)
    shutil.copy2(sidecar, release_dir / sidecar.name)
    return Fixture(app, version, commit, run_id, attempt, candidate_dir, release_dir, manifest_path, receipt, sidecar)


def artifacts_command(fx: Fixture, **overrides: str) -> List[str]:
    values = {
        "commit": fx.commit,
        "run_id": fx.run_id,
        "attempt": fx.attempt,
        "candidate_dir": str(fx.candidate_dir),
        "release_dir": str(fx.release_dir),
        "receipt": str(fx.receipt),
        "sidecar": str(fx.sidecar),
    }
    values.update(overrides)
    return [
        "bash", str(SHARED_ARTIFACT_VERIFIER),
        "--app", fx.app.name,
        "--repo", REPO,
        "--tag", f"{fx.app.name}@{fx.version}",
        "--commit", values["commit"],
        "--candidate-run-id", values["run_id"],
        "--candidate-run-attempt", values["attempt"],
        "--mode", "fixture",
        "--candidate-receipt-dir", values["candidate_dir"],
        "--release-assets-dir", values["release_dir"],
        "--gke-receipt", values["receipt"],
        "--gke-receipt-sidecar", values["sidecar"],
    ]


def candidate_local_command(fx: Fixture, **overrides: str) -> List[str]:
    values = {
        "manifest": str(fx.manifest),
        "sidecar": str(fx.manifest.with_name(fx.manifest.name + ".sha256")),
        "commit": fx.commit,
    }
    values.update(overrides)
    return [
        "bash", str(SHARED_CANDIDATE_VERIFIER),
        "--app", fx.app.name,
        "--repo", REPO,
        "--version", fx.version,
        "--commit", values["commit"],
        "--run-id", fx.run_id,
        "--run-attempt", fx.attempt,
        "--manifest", values["manifest"],
        "--manifest-sidecar", values["sidecar"],
        "--artifacts-dir", str(fx.candidate_dir),
        "--mode", "local",
    ]


def run_fixture(app: App, maker) -> Tuple[int, int]:
    positives = negatives = 0
    with tempfile.TemporaryDirectory(prefix=f"release-contract-{app.name}-") as tmp:
        root = Path(tmp)
        fx = build_fixture(app, root, maker)
        run_command(candidate_local_command(fx), label=f"{app.name} candidate fixture (local mode)", expect_ok=True, must_print=FIXTURE_BANNER)
        positives += 1
        run_command(artifacts_command(fx), label=f"{app.name} release fixture", expect_ok=True, must_print=FIXTURE_BANNER)
        positives += 1

        run_command(artifacts_command(fx, commit="e" * 40), label=f"{app.name} fixture: commit differs from the candidate", expect_ok=False)
        run_command(artifacts_command(fx, attempt="1"), label=f"{app.name} fixture: candidate run attempt differs", expect_ok=False)

        bad = root / "bad-result"
        bad.mkdir()
        flipped = json.loads(fx.receipt.read_text())
        flipped["result"] = "failed"
        bad_receipt = bad / app.receipt_name
        write_json(bad_receipt, flipped)
        bad_sidecar = write_sidecar(bad_receipt)
        run_command(artifacts_command(fx, receipt=str(bad_receipt), sidecar=str(bad_sidecar)), label=f"{app.name} fixture: receipt result is not passed", expect_ok=False)

        tampered = root / "tampered"
        tampered.mkdir()
        tampered_receipt = tampered / app.receipt_name
        tampered_receipt.write_text(fx.receipt.read_text() + "\n")
        run_command(artifacts_command(fx, receipt=str(tampered_receipt)), label=f"{app.name} fixture: receipt bytes differ from its sidecar", expect_ok=False)

        missing = root / "missing-asset"
        shutil.copytree(fx.release_dir, missing)
        (missing / f"{app.name}-{app.targets[0]}.tar.gz.sha256").unlink()
        run_command(artifacts_command(fx, release_dir=str(missing)), label=f"{app.name} fixture: a release asset is missing", expect_ok=False)

        swapped = root / "swapped-archive"
        shutil.copytree(fx.release_dir, swapped)
        archive = swapped / f"{app.name}-{app.targets[0]}.tar.gz"
        archive.write_bytes(archive.read_bytes() + b"\n")
        run_command(artifacts_command(fx, release_dir=str(swapped)), label=f"{app.name} fixture: a public archive differs from the candidate bytes", expect_ok=False)

        stale = root / "stale-sidecar"
        stale.mkdir()
        stale_sidecar = stale / "final-candidate-manifest.json.sha256"
        stale_sidecar.write_text(f"{'0' * 64}  final-candidate-manifest.json\n")
        run_command(candidate_local_command(fx, sidecar=str(stale_sidecar)), label=f"{app.name} candidate fixture: manifest sidecar does not match", expect_ok=False)
        negatives += 7
    return positives, negatives


# --- drivers -----------------------------------------------------------------


def watched_files() -> List[Path]:
    paths = [APPS_SH, RECEIPT_MAKER, SHARED_CANDIDATE_VERIFIER, SHARED_ARTIFACT_VERIFIER, CI_WORKFLOW]
    for app in APPS.values():
        if app.onboarded:
            paths.extend([app.candidate_workflow, app.promotion_workflow])
    return paths


def check_everything() -> str:
    onboarded = [app for app in APPS.values() if app.onboarded]
    for app in APPS.values():
        if app.onboarded:
            check_app(app)
        else:
            check_not_onboarded(app)
    check_tables(load_receipt_maker())
    check_standalone_gates(read(APPS["defer"].candidate_workflow), read(CI_WORKFLOW))
    return f"release contract passed: {' '.join(app.name for app in onboarded)}, standalone gates, PR CI, release tables"


def candidate_controls(app: App, text: str) -> List[Tuple[str, Callable[[], str]]]:
    label = f"{app.name} candidate"
    return [
        (f"{label}: commit input renamed", lambda: replace_once(text, "      commit:\n", "      commit_sha:\n", label)),
        (f"{label}: result job renamed", lambda: replace_once(text, "\n  result:\n", "\n  results:\n", label)),
        (f"{label}: action unpinned", lambda: unpin_first_action(text, label)),
        (f"{label}: manifest schema bumped silently", lambda: replace_all(text, app.manifest_schema, app.manifest_schema[:-1] + "9", label)),
        (f"{label}: verifier downgraded to local mode", lambda: replace_all(text, "--mode full", "--mode local", label)),
        (f"{label}: commit identity proof dropped", lambda: replace_all(text, '[[ "$REQUESTED_COMMIT" == "$GITHUB_SHA" ]]', "true", label)),
        (f"{label}: build result binding faked", lambda: replace_all(text, 'build:"${{ needs.build.result }}"', 'build:"success"', label)),
        (f"{label}: main-only guard dropped", lambda: replace_all(text, "refs/heads/main", "refs/heads/release", label)),
        (f"{label}: extra trigger added", lambda: replace_once(text, "on:\n  workflow_dispatch:\n", "on:\n  push:\n    branches: [main]\n  workflow_dispatch:\n", label)),
        (f"{label}: stale target archive still downloaded", lambda: replace_all(text, f"{app.name}-candidate-{app.targets[-1]}-", f"{app.name}-candidate-x86_64-unknown-freebsd-", label)),
        (f"{label}: manifest loop drops a target", lambda: replace_all(text, f"for target in {' '.join(app.targets)}; do", f"for target in {' '.join(app.targets[:-1])}; do", label)),
        (f"{label}: build matrix row dropped", lambda: drop_matrix_row(app, text, label)),
    ]


def promotion_controls(app: App, text: str) -> List[Tuple[str, Callable[[], str]]]:
    label = f"{app.name} promotion"
    tag_ref = f"refs/tags/{app.name}@${{{{ inputs.version }}}}"

    def retag_from_mutable() -> str:
        lines = text.splitlines(keepends=True)
        for index, line in enumerate(lines):
            if "docker buildx imagetools create" in line and line.rstrip("\n").endswith('"$root_ref"'):
                lines[index] = line.rstrip("\n")[: -len('"$root_ref"')] + '"$candidate_ref"\n'
                return "".join(lines)
        fail(f"{label}: no retag line to mutate")

    controls: List[Tuple[str, Callable[[], str]]] = [
        (f"{label}: tag push trigger added", lambda: replace_once(text, "on:\n  workflow_dispatch:\n", f"on:\n  push:\n    tags: ['{app.name}@*']\n  workflow_dispatch:\n", label)),
        (f"{label}: candidate run id input dropped", lambda: replace_once(text, "      candidate_run_id:\n", "      candidate_run:\n", label)),
        (f"{label}: publish job renamed", lambda: replace_once(text, "\n  publish-release:\n", "\n  publish:\n", label)),
        (f"{label}: action unpinned", lambda: unpin_first_action(text, label)),
        (f"{label}: rebuild inserted", lambda: insert_after_first_run_block(text, "docker build .", label)),
        (f"{label}: retag sourced from a mutable ref", retag_from_mutable),
        (f"{label}: public verification dropped", lambda: replace_all(text, "--mode public", "--mode candidate", label)),
        (f"{label}: tag ref guard dropped", lambda: replace_all(text, tag_ref, "refs/heads/main", label)),
    ]
    if app.shared_scripts:
        compatibility = bash_variable("RELEASE_COMPATIBILITY_LINE")
        controls.append((f"{label}: compatibility line reworded", lambda: replace_once(text, compatibility, "- Compatibility: unchanged.", label)))
    return controls


def standalone_controls(candidate: str, ci: str) -> List[Tuple[str, Callable[[], None]]]:
    def gates_with(candidate_text: str = candidate, ci_text: str = ci) -> Callable[[], None]:
        return lambda: check_standalone_gates(candidate_text, ci_text)

    controls: List[Tuple[str, Callable[[], None]]] = []
    for gate in STANDALONE_GATES:
        controls.append((f"defer candidate: gate dropped: {gate}", gates_with(candidate_text=replace_once(candidate, gate, "true", "defer candidate"))))
        controls.append((f"defer-ci: gate dropped: {gate}", gates_with(ci_text=replace_once(ci, gate, "true", "defer-ci"))))
    controls.extend([
        ("defer candidate: release contract no longer checked", gates_with(candidate_text=replace_once(candidate, "verify-release-contract.py --app defer", "verify-release-contract.py --self-test", "defer candidate"))),
        ("defer-ci: pull_request trigger dropped", gates_with(ci_text=replace_once(ci, "  pull_request:\n", "  workflow_dispatch:\n", "defer-ci"))),
        ("defer-ci: push no longer limited to main", gates_with(ci_text=replace_once(ci, "    branches: [main]", "    branches: ['**']", "defer-ci"))),
        ("defer-ci: action unpinned", gates_with(ci_text=unpin_first_action(ci, "defer-ci"))),
        ("defer-ci: write permission granted", gates_with(ci_text=replace_once(ci, "permissions:\n  contents: read", "permissions:\n  contents: write", "defer-ci"))),
    ])
    return controls


def self_test() -> str:
    before = {path: sha256(path) for path in watched_files()}
    positives = negatives = 0
    onboarded = [app for app in APPS.values() if app.onboarded]
    for app in onboarded:
        check_app(app)
        positives += 1
    maker = load_receipt_maker()
    check_tables(maker)
    positives += 1
    candidate_text = read(APPS["defer"].candidate_workflow)
    ci_text = read(CI_WORKFLOW)
    check_standalone_gates(candidate_text, ci_text)
    positives += 1
    for app in APPS.values():
        if not app.onboarded:
            check_not_onboarded(app)

    for app in onboarded:
        candidate = read(app.candidate_workflow)
        promotion = read(app.promotion_workflow)
        for label, mutate in candidate_controls(app, candidate):
            expect_static_failure(label, lambda mutate=mutate, app=app: check_candidate(app, mutate()))
            negatives += 1
        for label, mutate in promotion_controls(app, promotion):
            expect_static_failure(label, lambda mutate=mutate, app=app: check_promotion(app, mutate()))
            negatives += 1
    for label, control in standalone_controls(candidate_text, ci_text):
        expect_static_failure(label, control)
        negatives += 1
    expect_static_failure("table: a non-onboarded app is checked as onboarded", lambda: check_app(replace(APPS["defer"], onboarded=False)))
    negatives += 1

    fixture_names = []
    for name in FIXTURE_APPS:
        pos, neg = run_fixture(APPS[name], maker)
        positives += pos
        negatives += neg
        fixture_names.append(name)

    after = {path: sha256(path) for path in watched_files()}
    if before != after:
        changed = [str(path.relative_to(ROOT)) for path in before if before[path] != after[path]]
        fail(f"self-test must not rewrite checked files: {changed}")
    return (
        f"release contract self-test passed: {positives} positive checks, "
        f"{negatives} negative controls refused, fixtures {' '.join(fixture_names)}"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--app", choices=sorted(APPS), help="check one app's candidate and promotion workflows")
    parser.add_argument("--self-test", action="store_true", help="run positive checks, negative controls, and the offline fixtures")
    args = parser.parse_args(argv)
    try:
        if args.self_test and args.app:
            parser.error("--self-test covers every onboarded app; drop --app")
        if args.self_test:
            summary = self_test()
        elif args.app:
            app = APPS[args.app]
            check_app(app)
            check_tables(load_receipt_maker())
            check_standalone_gates(read(app.candidate_workflow), read(CI_WORKFLOW))
            summary = f"release contract passed: {app.name}"
        else:
            summary = check_everything()
    except ContractError as exc:
        print(f"verify-release-contract: refused: {exc}", file=sys.stderr)
        return 1
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
