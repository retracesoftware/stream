#!/bin/bash
set -e

# Build release version
echo "=== Building RELEASE version ==="
docker run --rm \
    -v $(pwd):/retrace -v $(pwd)/dist:/output -w /retrace \
    -v ../functional:/retrace/functional \
    -v ../utils:/retrace/utils \
    retrace-build:latest \
    pip wheel -Csetup-args="-Dbuildtype=release" --find-links /retrace/functional/dist --find-links /retrace/utils/dist --no-build-isolation --wheel-dir=/output .

# Build debug version using the debug pyproject.toml
echo "=== Building DEBUG version ==="
docker run --rm \
    -e MESON_ARGS='--verbose -Dbuildtype=debug -Ddebug_build=true' \
    -v $(pwd):/retrace -v $(pwd)/dist:/output -w /retrace \
    -v ../functional:/retrace/functional \
    -v ../utils:/retrace/utils \
    retrace-build:latest \
    bash -c "cp pyproject.debug.toml pyproject.toml.bak && \
             cp pyproject.toml pyproject.toml.orig && \
             cp pyproject.debug.toml pyproject.toml && \
             pip wheel -Csetup-args='-Dbuildtype=debug' -Csetup-args='-Ddebug_build=true' --find-links /retrace/functional/dist --find-links /retrace/utils/dist --no-build-isolation --wheel-dir=/output . ; \
             cp pyproject.toml.orig pyproject.toml"

echo "=== Build complete ==="
echo "Wheels in dist/:"
ls -la dist/*.whl
