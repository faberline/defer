import importlib.util
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import handoff
spec = importlib.util.spec_from_file_location('policy', ROOT / 'scripts/issue-ci/check-policy.py')
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)

class ConsumerPolicyTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / '.github/issue-ci-required-gates.json').read_text())

    def test_exact_existing_gate_inventory(self):
        expected = [{'path': '.github/workflows/defer-ci.yml', 'jobs': ['fmt, clippy, tests, release contract']}, {'path': '.github/workflows/gke-test.yml', 'jobs': ['build server image / build + publish ghcr test image', 'build test-client image', 'resolve image identity', 'GKE e2e (operator_recovery) / deploy + test on shared GKE']}]
        self.assertEqual(self.config['workflows'], expected)

    def test_deployed_shared_action_interface(self):
        workflow = (ROOT / '.github/workflows/issue-ci.yml').read_text()
        self.assertIn('workflows: ["defer-ci", "gke-test"]', workflow)
        self.assertIn('uses: faberline/aw/.github/actions/issue-ci@693e45bded9a4ec8167573e82b423c823c193ec0', workflow)
        self.assertIn('operation: handoff', workflow)
        self.assertIn('run-id: ${{ github.event.workflow_run.id }}', workflow)
        self.assertIn('policy: .github/issue-ci-required-gates.json', workflow)
        self.assertNotIn('config:', workflow)
        self.assertIn('github.event.workflow_run.event == \'pull_request\'', workflow)
        self.assertIn('cancel-in-progress: false', workflow)
        self.assertNotIn('pull_request_target', workflow)
        self.assertNotIn('download-artifact', workflow)

    def test_aw_revision_pinned_to_pilot_commit(self):
        self.assertEqual(self.config.get('aw_revision'), '693e45bded9a4ec8167573e82b423c823c193ec0')

    def test_qualification_contract_v1_schema(self):
        qual = self.config.get('qualification')
        self.assertIsInstance(qual, dict)
        self.assertEqual(qual.get('version'), 1)
        allowed = {w['path']: set(w['jobs']) for w in self.config['workflows']}

        # Trusted workflows must match allowlist with numeric id > 0 and 40-hex blob SHA
        trusted = qual.get('trusted_workflows')
        self.assertIsInstance(trusted, dict)
        self.assertEqual(set(trusted.keys()), set(allowed.keys()))
        for path, info in trusted.items():
            self.assertIsInstance(info['id'], int)
            self.assertGreater(info['id'], 0)
            self.assertTrue(re.fullmatch('[0-9a-f]{40}', info['definition_sha']))

        # Checks must map to allowed workflow paths and jobs
        checks = qual.get('checks')
        self.assertIsInstance(checks, dict)
        self.assertTrue(checks)
        valid_kinds = {'unit', 'functional_e2e', 'performance', 'policy'}
        for name, check in checks.items():
            self.assertTrue(re.fullmatch(r'[A-Za-z0-9_.-]+', name))
            self.assertIn(check.get('kind'), valid_kinds)
            self.assertIn(check.get('workflow'), allowed)
            self.assertIn(check.get('job'), allowed[check['workflow']])

        # Always must name real policy checks
        always = qual.get('always')
        self.assertIsInstance(always, list)
        self.assertTrue(always)
        for name in always:
            self.assertIn(name, checks)
            self.assertEqual(checks[name]['kind'], 'policy')

        # Protected paths must be safe and include gate infrastructure
        protected = qual.get('protected_paths')
        self.assertIsInstance(protected, list)
        self.assertIn('.github/issue-ci-required-gates.json', protected)
        self.assertIn('scripts/issue-ci/**', protected)

        # Rules must be nonempty and cover path globs with valid require and exempt
        rules = qual.get('rules')
        self.assertIsInstance(rules, list)
        self.assertTrue(rules)
        categories = {'unit', 'functional_e2e', 'performance'}
        for rule in rules:
            self.assertIsInstance(rule['paths'], list)
            self.assertTrue(rule['paths'])
            required = rule.get('require', [])
            exempt = rule.get('exempt', {})
            for name in required:
                self.assertIn(name, checks)
            for cat, reason in exempt.items():
                self.assertIn(cat, categories)
                self.assertTrue(isinstance(reason, str) and reason.strip())
            kinds = {checks[name]['kind'] for name in required}
            self.assertFalse(kinds & set(exempt.keys()))

    def test_every_real_workflow_and_job_is_required(self):
        gates = self.config['workflows']
        snapshots = {}
        for i, gate in enumerate(gates):
            run = dict(id=i, status='completed', conclusion='success', html_url=f'https://github.com/o/r/actions/runs/{i}')
            jobs = [dict(name=name, status='completed', conclusion='success', html_url=f'{run["html_url"]}/job/{j}') for j, name in enumerate(gate['jobs'])]
            snapshots[gate['path']] = run, jobs
        self.assertEqual(handoff.evaluate(gates, snapshots)[0], 'done')
        for gate in gates:
            path = gate['path']
            missing = dict(snapshots); missing.pop(path)
            self.assertEqual(handoff.evaluate(gates, missing)[0], 'pending')
            for index in range(len(gate['jobs'])):
                jobs = list(snapshots[path][1]); jobs.pop(index)
                missing = dict(snapshots); missing[path] = snapshots[path][0], jobs
                self.assertEqual(handoff.evaluate(gates, missing)[0], 'pending')
                for conclusion in ('skipped', 'cancelled', 'neutral'):
                    jobs = [dict(j) for j in snapshots[path][1]]; jobs[index]['conclusion'] = conclusion
                    missing[path] = snapshots[path][0], jobs
                    self.assertEqual(handoff.evaluate(gates, missing)[0], 'pending')

    def test_current_failure_only_policy_is_valid(self):
        self.assertEqual(policy.check(ROOT / '.github/issue-ci-required-gates.json'), self.config)
        self.assertEqual(self.config['lifecycle_mode'], 'failure-only')
        self.assertTrue(self.config['qualification_blockers'])

    def test_guard_refuses_full_or_missing_policy_and_old_controller(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'policy.json'
            for mode in ('full', 'invalid', None):
                config = dict(self.config, lifecycle_mode=mode)
                path.write_text(json.dumps(config))
                with self.assertRaises(RuntimeError): policy.check(path)
            for blockers in (None, 'approved', [], [1], ['']):
                path.write_text(json.dumps(dict(self.config, qualification_blockers=blockers)))
                with self.assertRaises(RuntimeError): policy.check(path)
            config = dict(self.config); config.pop('qualification_blockers')
            path.write_text(json.dumps(config))
            with self.assertRaises(RuntimeError): policy.check(path)
            with patch.object(policy, 'load_controller', return_value=object()):
                with self.assertRaisesRegex(RuntimeError, 'policy-aware controller'):
                    policy.check(path)

    def test_real_gate_failure_only_and_success_never_done(self):
        snapshots = {}
        for i, gate in enumerate(self.config['workflows']):
            run = dict(id=i, status='completed', conclusion='success', html_url=f'https://github.com/o/r/actions/runs/{i}')
            snapshots[gate['path']] = run, [dict(name=name, status='completed', conclusion='success', html_url=run['html_url'] + '/job/1') for name in gate['jobs']]
        self.assertEqual(handoff.evaluate_policy(self.config, snapshots)[0], 'pending')
        path = self.config['workflows'][0]['path']
        snapshots[path][1][0]['conclusion'] = 'failure'
        state, reasons, _ = handoff.evaluate_policy(self.config, snapshots)
        self.assertEqual(state, 'todo')
        self.assertTrue(any(snapshots[path][1][0]['html_url'] in reason for reason in reasons))

if __name__ == '__main__':
    unittest.main()
