#!/usr/bin/env python3
"""Offline release fixture for defer's candidate-first release contract.

defer releases as landed main -> immutable candidate -> digest-pinned GKE
acceptance -> protected annotated tag -> no-rebuild promotion. This script
builds a fake candidate and release in a temp dir, then runs
`verify-release-candidate.sh` and `verify-release-artifacts.sh` against it:
2 positive and 7 negative cases. The static rules are
`aw release-contract --app defer`.

Usage:
  verify-release-contract.py --self-test   the offline release fixture

The checks never rewrite a checked file.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, NoReturn, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
RELEASE_DIR = ROOT / "scripts" / "release"
APPS_SH = RELEASE_DIR / "apps.sh"
RECEIPT_MAKER = RELEASE_DIR / "make-gke-release-receipt.py"
SHARED_CANDIDATE_VERIFIER = RELEASE_DIR / "verify-release-candidate.sh"
SHARED_ARTIFACT_VERIFIER = RELEASE_DIR / "verify-release-artifacts.sh"

REPO = "faberline/defer"
IMAGE_OWNER = "ghcr.io/faberline"
FIXTURE_BANNER = "LOCAL FIXTURE ONLY"

TWO_TARGETS = ("x86_64-unknown-linux-musl", "aarch64-unknown-linux-musl")


@dataclass(frozen=True)
class App:
    name: str
    targets: Tuple[str, ...]
    manifest_schema: str
    receipt_name: str
    receipt_schema: str
    gke_backend: str


def shared_app(name: str, targets: Tuple[str, ...], backend: str) -> App:
    return App(
        name=name,
        targets=targets,
        manifest_schema=f"cclab.{name}.candidate-manifest.v1",
        receipt_name=f"{name}-gke-receipt.json",
        receipt_schema=f"{name}.gke-release-receipt/v1",
        gke_backend=backend,
    )


APPS: Dict[str, App] = {
    "defer": shared_app("defer", TWO_TARGETS, "gke-acceptance"),
}
FIXTURE_APPS = ("defer",)
FIXTURE_VERSION = {"defer": "0.4.13"}


# --- helpers -----------------------------------------------------------------


class ContractError(Exception):
    pass


def fail(message: str) -> NoReturn:
    raise ContractError(message)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def load_receipt_maker():
    spec = importlib.util.spec_from_file_location("make_gke_release_receipt", RECEIPT_MAKER)
    if spec is None or spec.loader is None:
        fail(f"cannot load {RECEIPT_MAKER.relative_to(ROOT)}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
    return [APPS_SH, RECEIPT_MAKER, SHARED_CANDIDATE_VERIFIER, SHARED_ARTIFACT_VERIFIER]


def self_test() -> str:
    before = {path: sha256(path) for path in watched_files()}
    positives = negatives = 0
    maker = load_receipt_maker()
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
    parser.add_argument("--self-test", action="store_true", help="run the offline release fixture and check that the run rewrites no file")
    args = parser.parse_args(argv)
    if not args.self_test:
        parser.error("pass --self-test; the static checks are aw release-contract --app defer")
    try:
        summary = self_test()
    except ContractError as exc:
        print(f"verify-release-contract: refused: {exc}", file=sys.stderr)
        return 1
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
