# kiok-pack

DAG package for the [kiok](https://github.com/cloudcheflabs) workflow orchestrator.

A kiok DAG can reach the cluster three ways — **git-sync**, **bundle upload**, or
**manual registration** — and this repository is the reference layout for the
first two.

## Repository layout (conventions)

```
src/main/script/   YAML DAG definitions      ← git-sync scans this path
src/main/java/     Java sources (kiok-sdk)   ← compiled into build/dag.jar
src/main/python/   Python sources            ← helper modules for python tasks
lib/java/          vendored *.jar deps       ← air-gapped Java dependencies
lib/python/        vendored *.whl deps       ← air-gapped Python dependencies
build/dag.jar      pre-built bundle jar      ← classes for type:java tasks
```

These paths are **conventions** — kiok does not let you reconfigure them:

| What | Path | Notes |
|------|------|-------|
| YAML DAGs | `src/main/script` | the only git-sync setting (`subpath`) points here |
| Java code | `src/main/java` | committed pre-built as `build/dag.jar` |
| Python code | `src/main/python` | helper modules; tasks may also inline a script |
| Vendored deps | `lib/java`, `lib/python` | air-gapped: commit the actual jar/whl files |

## DAG identity

Every DAG has a human **name** (`dag.id` in the YAML — need not be unique) and a
globally-unique **id** derived from its origin:

- git    → `<repo>/<sourcePath>` → slug, e.g. `kiok-pack_src_main_script_daily_etl`
- bundle → `<bundleName>/<sourcePath>` → slug
- manual → the name itself

So two DAGs may share a name as long as they come from different repos/bundles.

## Task types

- `shell` — inline shell script
- `python` — inline python script (`type: python`)
- `python_callable` — `config.module` + `config.callable`
- `http` — `config.url`
- `java` — `config.class` (a main class in the bundle `dag.jar`)

## Ingestion paths

**git-sync** — register this repo in the admin UI under **Settings → Git Sync**:
- URL: `https://github.com/cloudcheflabs/kiok-pack.git`
- Ref: a branch (`master`) or a tag (`v1.0.0`)
- Subpath: `src/main/script`
- Private repos: auth is supplied by a kiok **Connection whose id equals the
  repo name** (`privateKey`, or `username` + `password`). git-sync configuration
  itself never holds credentials.

**bundle upload** — for fully air-gapped clusters, zip the artifacts and upload
under **Settings → DAG Bundles**. A bundle zip contains `*.yaml` DAG definitions
and optionally a `dag.jar`. Re-uploading the same bundle name overwrites it.

**manual** — register a single YAML DAG directly via the admin UI / API.

## Storage

git DAGs are re-derived from the repo on each sync. **bundle and manual DAGs are
persisted in the master's RocksDB metadata store**; enable Backup to flush them
(with all cluster state) to S3.

## Never commit credentials

This is a public repository. Never commit access keys, tokens, passwords or
private keys. Credentials live only in kiok's KMS-encrypted connection store;
DAGs reference them indirectly via `${conn.<id>.<key>}` / `${secret.<name>}`.
