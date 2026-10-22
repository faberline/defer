# API and clients

One HTTP contract, served and printed from one source, with clients generated
from it. This area is the README capability `api-cli-clients`.

## One discoverable HTTP contract

- Problem: none open as shipped.
- Who: client authors; agents driving the CLI.
- Promise: the node serves exactly the routes the OpenAPI document lists —
  queue get, put, and control; task create, batch create, status, and
  cancel; dispatch; admin backup — plus the probes, over HTTP/1.1 and h2c on
  one port; `defer spec` prints the same document offline and
  `--format routes` lists the routes; `defer spec gen --lang ts`, `py`, or
  `rust` emits a client scoped to those routes; the CLI ships `llm`,
  `upgrade`, and `issue` plus the queue, task, dispatch, backup, k8s, and
  dockerfile verbs under the shared conventions, and the offline verbs stay
  within the release-mode latency and resource ceilings.
- Contract snapshot: [`clients/openapi.json`](../../clients/README.md) is
  committed; a unit test in `defer-queue` refuses drift from the live document.
- Limits today: clients are generated, not committed; the dispatch response
  schema is published under the `crate.DispatchReport` name; the efficiency
  and stability gates are release-mode and ignored by default.
- Non-goals: a committed client package per language.
- Neighbours: the listing operations from
  [tasks-and-queues.md](tasks-and-queues.md) § Resource listing and
  retention land in this contract when that outcome ships.
- Status rows: `openapi-and-generated-clients`, `cli-surface`.

## Non-goals in this area

- `cloud-tasks-wire-compatibility`: defer's REST surface is its own; the
  `cloudtasks.googleapis.com` REST and gRPC shapes are not served.
