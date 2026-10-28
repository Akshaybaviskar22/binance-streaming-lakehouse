#!/usr/bin/env bash
# Download the Flink Kafka SQL connector into docker/flink-lib/.
# Mounted into both Flink containers by docker-compose.yml (profile: flink).
#
# Usage: ./flink-setup.sh [flink_version] [connector_version]
#   ./flink-setup.sh                    # defaults: 1.20  3.4.0-1.20
#   ./flink-setup.sh 1.20 3.4.0-1.20

set -euo pipefail

FLINK_VERSION="${1:-1.20}"
CONNECTOR_VERSION="${2:-3.4.0-1.20}"

HERE="$(cd "$(dirname "$0")" && pwd)"
LIB_DIR="$HERE/flink-lib"
mkdir -p "$LIB_DIR"

JAR="flink-sql-connector-kafka-${CONNECTOR_VERSION}.jar"
URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${CONNECTOR_VERSION}/${JAR}"

echo "Downloading $JAR ..."
curl -fL --retry 3 -o "$LIB_DIR/$JAR" "$URL"
echo "Saved → $LIB_DIR/$JAR"
echo "Now run: make flink-up"
