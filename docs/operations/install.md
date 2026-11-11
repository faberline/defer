# Install and quickstart

This guide takes you from a checkout to one running node, a task that reaches
`Succeeded`, and a 3-replica Kubernetes install. Every command here is checked
against `clients/openapi.json` or run against a local `defer serve`.

## Version

The code on `main` is version `0.4.13`. `defer --version` prints it. No release
tag or image exists yet; the first one is tracked by issue #108. Until then,
build from source. The images named `defer:0.4.13` in `k8s/` are only local
tags: build and load them yourself. The from-source image is built with
`docker build -f Dockerfile -t defer:0.4.13 .`. `Dockerfile.release` needs a
published release.

## Build the binary

```bash
cargo build --release --locked -p defer --bin defer
target/release/defer --version
```

The binary is `target/release/defer`. `./build.sh debug` also builds it and
installs it to `~/.cargo/bin/defer`. `./build.sh release` is for the release
flow: it edits the `k8s/` image pins, so use the plain `cargo build` above for
a local install.

## Run one node

In the first shell:

```bash
target/release/defer serve --bind 127.0.0.1:7141 --data-dir /tmp/defer/data
```

`--data-dir` holds the Raft log and snapshots. Auth is off by default, so the
quickstart needs no token. `/healthz` answers while the process is alive.
`defer serve --help` lists every flag, and the variable table in
[README.md](README.md) of this directory gives each one's default.

## Quickstart

Start the three shells in this order. The node above is the first one.

1. In a second shell, start a local target that answers 200 and prints each
   call it receives. Keep it running:

   ```bash
   python3 -c 'import http.server as h
   class H(h.BaseHTTPRequestHandler):
       def do_POST(s):
           s.rfile.read(int(s.headers.get("content-length",0))); print("target got", s.path, flush=True); s.send_response(200); s.end_headers()
   h.HTTPServer(("127.0.0.1",9000),H).serve_forever()'
   ```

2. In a third shell, create the queue `jobs`:

   ```bash
   curl -sS -X PUT http://127.0.0.1:7141/v1/queues/jobs \
     -H 'content-type: application/json' \
     -d '{"max_in_flight":10,"max_dispatch_per_tick":100,"max_dispatches_per_second":100,"max_burst_size":100,"lease_ttl_ms":30000,"retry_backoff_ms":1000}'
   ```

3. Create the task `hello-1`, due now, aimed at the local target:

   ```bash
   curl -sS -X POST http://127.0.0.1:7141/v1/queues/jobs/tasks \
     -H 'content-type: application/json' \
     -d "{\"task_id\":\"hello-1\",\"schedule_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"target\":{\"url\":\"http://127.0.0.1:9000/hook\"},\"payload\":{\"hello\":\"world\"}}"
   ```

4. Read the task. After a moment the dispatch tick (100 ms by default) runs it,
   so the status reads `Succeeded`, and the target shell prints
   `target got /hook`:

   ```bash
   curl -sS http://127.0.0.1:7141/v1/queues/jobs/tasks/hello-1
   ```

The paths, methods and field names above match `clients/openapi.json`:
`PUT /v1/queues/{queue}`, `POST /v1/queues/{queue}/tasks` and
`GET /v1/queues/{queue}/tasks/{task_id}`.

## Run three replicas on Kubernetes

You need a cluster, `kubectl`, a `defer` image that the cluster can pull, and
an operator image.

1. Install the custom resource definition:

   ```bash
   target/release/defer k8s crd render | kubectl apply -f -
   ```

2. Set the image in `k8s/operator/deployment.yaml`, then apply the operator:

   ```bash
   kubectl apply -k k8s/operator
   ```

3. Create the three secrets that the `prod` profile names:
   `defer-token-registry`, `defer-target-signing` and `defer-peer-tls`. Each is
   a Kubernetes `Secret`. For what each one holds, see `defer serve --help` and
   [README.md](README.md) of this directory.

4. Render the `Defer` resource for the `prod` profile, set your image, and
   apply it:

   ```bash
   target/release/defer k8s instance render --profile prod --name defer --namespace production --image <your image> --out defer.yaml
   ```

   Edit `backup.destination` in `defer.yaml` (the render prints
   `s3://REPLACE_ME/defer`), then:

   ```bash
   kubectl apply -f defer.yaml
   ```

5. Check that the three replicas are ready:

   ```bash
   kubectl -n production get statefulset defer
   ```

   The ready count reads `3/3`.

For one replica without the operator, use `kubectl apply -k k8s/overlays/dev`.
The direct-install base in `k8s/base/` is a singleton (`replicas: 1`), not a
3-replica group; only the operator creates a 3-replica group.

Limit: Raft membership is static, set from `DEFER_PEERS`. Changing the replica
count is not supported live, and it restarts members (see the
`replica-membership` row in [STATUS.md](../../STATUS.md)).

## Next

- [README.md](../../README.md): what defer promises and its primary workflow.
- [STATUS.md](../../STATUS.md): what is implemented and what is not.
- [README.md](README.md) of this directory: the serve settings and operations.
- [clients/README.md](../../clients/README.md): client libraries and the OpenAPI document.
