# kiok-pack

DAG package for the [kiok](https://github.com/cloudcheflabs) workflow orchestrator.

kiok's git-sync clones this repository and registers every DAG it finds.

## Layout

```
src/main/script/   YAML DAG definitions   (git-sync scans this path)
src/main/java/     Java DAG sources        (kiok-sdk; future bundle phase)
src/main/python/   Python DAG sources      (kiok-python-sdk; future bundle phase)
lib/java/          vendored *.jar deps     (air-gapped)
lib/python/        vendored *.whl deps     (air-gapped)
```

## Git sync

Register this repo in the kiok admin UI under **Settings -> Git Sync**:

- URL: `https://github.com/cloudcheflabs/kiok-pack.git`
- Ref: a branch (e.g. `master`) or a tag (e.g. `v1.0.0`)
- Subpath: `src/main/script`

Never commit credentials. Private-repo auth is supplied through a kiok
Connection whose id matches the repository name.
