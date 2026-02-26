#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

build_jni() {
  echo "=== Building spark-rapids-jni ==="
  cd "$SCRIPT_DIR/spark-rapids-jni"
  ./build/build-in-docker -DCPP_PARALLEL_LEVEL=8 -DskipTests clean install
}

build_rapids() {
  echo "=== Building spark-rapids ==="
  cd "$SCRIPT_DIR/spark-rapids"
  mvn clean install -DskipTests -Dbuildver=356 \
    -Dmaven.javadoc.skip=true \
    -Dmaven.scaladoc.skip=true \
    -Dmaven.scalastyle.skip=true \
    -Ddist.jar.compress=false \
    -Drat.skip=true
}

build_private() {
  echo "=== Building spark-rapids-private ==="
  cd "$SCRIPT_DIR/spark-rapids-private"
  mvn clean install -Dbuildver=356
}

if [[ $# -eq 0 ]]; then
  build_jni
  build_private
  build_rapids
elif [[ $# -eq 1 ]]; then
  case "$1" in
    jni)     build_jni ;;
    rapids)  build_rapids ;;
    private) build_private ;;
    *)
      echo "Unknown component: $1" >&2
      echo "Usage: $0 [jni|rapids|private]" >&2
      exit 1
      ;;
  esac
else
  echo "Usage: $0 [jni|rapids|private]" >&2
  exit 1
fi
