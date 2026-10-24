#!/usr/bin/env python3
"""Pinned aw@693e45bded9a4ec8167573e82b423c823c193ec0 evaluator & action boundary regressions.

This test harness loads and executes the ACTUAL pinned shared action modules
(isolated in scripts/issue-ci/fixtures/pinned_aw) under strict hash contracts,
verifying that the live consumer policy preserves full failure-only inventory
and behaves correctly at the evaluator and action boundaries without network access.
"""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = ROOT / 'scripts/issue-ci/fixtures/pinned_aw'

EXPECTED_HASHES = {
    'handoff.py': '8313b8c5bf88a42c4cd8b3131fc576360fb0d2b6d120aa30d1a820f4e5ada773',
    'qualification.py': 'acc235139940b4e5d8ea1e75b4101ac442ee14a1e77ea01207838a55fdc7eaed',
    'action.py': '4a6894fee5a1f56f2b469335c9e061ce39c51d7cb2220ee06fe680e6c3d51ab6',
}


def load_pinned_modules_from_tmp():
    """Copy pinned fixtures into an isolated temporary directory, verify hashes, and import."""
    tmp_dir = tempfile.mkdtemp(prefix='pinned-aw-evaluator-')
    tmp_path = Path(tmp_dir)
    loaded_hashes = {}
    for name, expected_sha in EXPECTED_HASHES.items():
        src = FIXTURES_DIR / name
        dst = tmp_path / name
        shutil.copy2(src, dst)
        actual_sha = hashlib.sha256(dst.read_bytes()).hexdigest()
        if actual_sha != expected_sha:
            shutil.rmtree(tmp_dir)
            raise ValueError(f'Hash mismatch for {name}: expected {expected_sha}, got {actual_sha}')
        loaded_hashes[name] = actual_sha

    # Load handoff
    spec_handoff = importlib.util.spec_from_file_location('pinned_handoff', tmp_path / 'handoff.py')
    pinned_handoff = importlib.util.module_from_spec(spec_handoff)
    spec_handoff.loader.exec_module(pinned_handoff)

    # Load action
    spec_action = importlib.util.spec_from_file_location('pinned_action', tmp_path / 'action.py')
    pinned_action = importlib.util.module_from_spec(spec_action)
    spec_action.loader.exec_module(pinned_action)

    # Load qualification sibling via handoff.qualification_module()
    pinned_qual = pinned_handoff.qualification_module()

    return tmp_dir, pinned_handoff, pinned_action, pinned_qual, loaded_hashes


class PinnedEvaluatorRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir, cls.handoff, cls.action, cls.qual, cls.hashes = load_pinned_modules_from_tmp()
        cls.live_policy_path = ROOT / '.github/issue-ci-required-gates.json'
        cls.live_config = json.loads(cls.live_policy_path.read_text())
        print(f"\n[PINNED EVALUATOR TEST HARNESS]")
        print(f"  handoff.py:       {cls.handoff.__file__} (SHA256: {cls.hashes['handoff.py']})")
        print(f"  qualification.py: {cls.qual.__file__} (SHA256: {cls.hashes['qualification.py']})")
        print(f"  action.py:        {cls.action.__file__} (SHA256: {cls.hashes['action.py']})")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_dir, ignore_errors=True)

    def test_hash_contract_matches_pilot_commit(self):
        """Verify the exact SHA256 hashes of the pinned aw modules."""
        for name, expected_sha in EXPECTED_HASHES.items():
            self.assertEqual(self.hashes[name], expected_sha, f'Hash mismatch on {name}')

    def test_deployed_scoping_path_cannot_narrow_live_inventory(self):
        """CURRENT live policy omits 'qualification', guaranteeing the deployed path cannot narrow inventory."""
        loaded = self.handoff.load_config(self.live_policy_path)
        self.handoff.validate_adoption(loaded)

        self.assertNotIn('qualification', loaded, 'Live policy must NOT contain qualification key')
        self.assertEqual(loaded['lifecycle_mode'], 'failure-only')
        self.assertTrue(loaded['qualification_blockers'])

        # Verify complete workflow inventory is preserved
        expected_workflows = [
            {'path': '.github/workflows/defer-ci.yml', 'jobs': ['fmt, clippy, tests, release contract']},
            {'path': '.github/workflows/gke-test.yml', 'jobs': [
                'build server image / build + publish ghcr test image',
                'build test-client image',
                'resolve image identity',
                'GKE e2e (operator_recovery) / deploy + test on shared GKE'
            ]}
        ]
        self.assertEqual(loaded['workflows'], expected_workflows)
        total_jobs = sum(len(w['jobs']) for w in loaded['workflows'])
        self.assertEqual(total_jobs, 5, 'Must evaluate all 5 gate jobs without live narrowing')

    def test_pinned_evaluator_defer_ci_success_and_gke_failure_returns_todo(self):
        """Actual pinned evaluator: defer-ci success + GKE failure -> state 'todo' with job link."""
        snapshots = {
            '.github/workflows/defer-ci.yml': (
                {'id': 101, 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/101'},
                [{'name': 'fmt, clippy, tests, release contract', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/101/job/1'}]
            ),
            '.github/workflows/gke-test.yml': (
                {'id': 102, 'status': 'completed', 'conclusion': 'failure', 'html_url': 'https://github.com/o/r/actions/runs/102'},
                [
                    {'name': 'build server image / build + publish ghcr test image', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/1'},
                    {'name': 'build test-client image', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/2'},
                    {'name': 'resolve image identity', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/3'},
                    {'name': 'GKE e2e (operator_recovery) / deploy + test on shared GKE', 'status': 'completed', 'conclusion': 'failure', 'html_url': 'https://github.com/o/r/actions/runs/102/job/4'}
                ]
            )
        }
        state, reasons, evidence = self.handoff.evaluate_policy(self.live_config, snapshots)
        self.assertEqual(state, 'todo')
        self.assertTrue(any('operator_recovery' in r and 'https://github.com/o/r/actions/runs/102/job/4' in r for r in reasons))
        self.assertEqual(evidence, ['101:1', '102:1'])

    def test_pinned_evaluator_gke_missing_inprogress_or_skipped_stays_pending(self):
        """Actual pinned evaluator: missing, in_progress, or skipped GKE runs remain 'pending'."""
        defer_ci_snapshot = (
            {'id': 101, 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/101'},
            [{'name': 'fmt, clippy, tests, release contract', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/101/job/1'}]
        )

        # 1. Missing GKE workflow run
        snapshots_missing = {'.github/workflows/defer-ci.yml': defer_ci_snapshot}
        state, reasons, _ = self.handoff.evaluate_policy(self.live_config, snapshots_missing)
        self.assertEqual(state, 'pending')
        self.assertIn('.github/workflows/gke-test.yml: missing run', reasons)

        # 2. In-progress GKE workflow run
        snapshots_in_progress = {
            '.github/workflows/defer-ci.yml': defer_ci_snapshot,
            '.github/workflows/gke-test.yml': (
                {'id': 102, 'status': 'in_progress', 'conclusion': None, 'html_url': 'https://github.com/o/r/actions/runs/102'},
                [{'name': 'GKE e2e (operator_recovery) / deploy + test on shared GKE', 'status': 'in_progress', 'conclusion': None, 'html_url': 'https://github.com/o/r/actions/runs/102/job/4'}]
            )
        }
        state, reasons, _ = self.handoff.evaluate_policy(self.live_config, snapshots_in_progress)
        self.assertEqual(state, 'pending')
        self.assertTrue(any('in_progress' in r for r in reasons))

        # 3. Skipped GKE job
        snapshots_skipped = {
            '.github/workflows/defer-ci.yml': defer_ci_snapshot,
            '.github/workflows/gke-test.yml': (
                {'id': 102, 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102'},
                [
                    {'name': 'build server image / build + publish ghcr test image', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/1'},
                    {'name': 'build test-client image', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/2'},
                    {'name': 'resolve image identity', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/3'},
                    {'name': 'GKE e2e (operator_recovery) / deploy + test on shared GKE', 'status': 'completed', 'conclusion': 'skipped', 'html_url': 'https://github.com/o/r/actions/runs/102/job/4'}
                ]
            )
        }
        state, reasons, _ = self.handoff.evaluate_policy(self.live_config, snapshots_skipped)
        self.assertEqual(state, 'pending')
        self.assertTrue(any('skipped' in r for r in reasons))

    def test_pinned_evaluator_all_green_in_failure_only_never_authorizes_done(self):
        """Actual pinned evaluator: all-green runs in failure-only mode return 'pending' with blockers."""
        snapshots_all_success = {
            '.github/workflows/defer-ci.yml': (
                {'id': 101, 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/101'},
                [{'name': 'fmt, clippy, tests, release contract', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/101/job/1'}]
            ),
            '.github/workflows/gke-test.yml': (
                {'id': 102, 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102'},
                [
                    {'name': 'build server image / build + publish ghcr test image', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/1'},
                    {'name': 'build test-client image', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/2'},
                    {'name': 'resolve image identity', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/3'},
                    {'name': 'GKE e2e (operator_recovery) / deploy + test on shared GKE', 'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/o/r/actions/runs/102/job/4'}
                ]
            )
        }
        state, reasons, evidence = self.handoff.evaluate_policy(self.live_config, snapshots_all_success)
        self.assertEqual(state, 'pending')
        self.assertIn('failure-only mode does not authorize done', reasons)
        for blocker in self.live_config['qualification_blockers']:
            self.assertIn(f'qualification blocked: {blocker}', reasons)

    def test_pinned_action_boundary_with_bounded_mocks(self):
        """Verify pinned action entrypoint (trusted_policy, validate, handoff) with bounded mocks."""
        ci = self.handoff
        live_bytes = self.live_policy_path.read_bytes()

        def fake_api(endpoint):
            if endpoint == 'repos/faberline/defer':
                return {'default_branch': 'main'}
            elif endpoint == 'repos/faberline/defer/commits/main':
                return {'sha': '35065bbbf26d435098490a835ef9393c8847b309'}
            elif endpoint.startswith('repos/faberline/defer/contents/.github/issue-ci-required-gates.json'):
                return {
                    'encoding': 'base64',
                    'type': 'file',
                    'content': base64.b64encode(live_bytes).decode()
                }
            elif endpoint == 'repos/faberline/defer/actions/runs/999':
                return {
                    'event': 'pull_request',
                    'head_repository': {'full_name': 'faberline/defer'},
                    'path': '.github/workflows/defer-ci.yml'
                }
            raise RuntimeError(f'Unexpected endpoint: {endpoint}')

        def fake_pages(endpoint, field=None):
            return []

        # 1. Test trusted_policy
        with patch.object(ci, 'api', side_effect=fake_api):
            content = self.action.trusted_policy(ci, 'faberline/defer', '.github/issue-ci-required-gates.json')
            self.assertEqual(content, live_bytes)

        # 2. Test action.main() with operation: validate
        env_validate = {
            'AW_ACTION_REF': '693e45bded9a4ec8167573e82b423c823c193ec0',
            'GITHUB_REPOSITORY': 'faberline/defer',
            'INPUT_OPERATION': 'validate',
            'INPUT_POLICY': '.github/issue-ci-required-gates.json',
            'PATH': os.environ.get('PATH', '')
        }
        with patch.dict(os.environ, env_validate, clear=True), \
             patch.object(self.action, 'controller', return_value=ci), \
             patch.object(ci, 'api', side_effect=fake_api):
            rc = self.action.main()
            self.assertEqual(rc, 0)

        # 3. Test action.main() with operation: handoff
        env_handoff = {
            'AW_ACTION_REF': '693e45bded9a4ec8167573e82b423c823c193ec0',
            'GITHUB_REPOSITORY': 'faberline/defer',
            'INPUT_OPERATION': 'handoff',
            'INPUT_RUN_ID': '999',
            'INPUT_POLICY': '.github/issue-ci-required-gates.json',
            'PATH': os.environ.get('PATH', '')
        }
        with patch.dict(os.environ, env_handoff, clear=True), \
             patch.object(self.action, 'controller', return_value=ci), \
             patch.object(ci, 'api', side_effect=fake_api), \
             patch.object(ci, 'pages', side_effect=fake_pages):
            rc = self.action.main()
            self.assertEqual(rc, 0)


if __name__ == '__main__':
    unittest.main()
