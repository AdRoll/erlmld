#!/bin/bash
#
#    Download the latest released AWS DynamoDB streams adapter and all required
#    dependencies (one of which is the KCL) to ./ddb_jars.  Similarly and separately
#    downloads KCL 1.9 and all required dependencies to ./kcl_jars.
#
#    Requires maven.
#
set -eu

cd "$(dirname $0)"
R="$(pwd)"

DYNAMO_PKG="dynamodb-streams-kinesis-adapter"
DYNAMO_VERSION="1.4.0"
DYNAMO_DESTDIR="$R/ddb_jars"

KCL_PKG="amazon-kinesis-client"
KCL_VERSION="1.9.1"
KCL_DESTDIR="$R/kcl_jars"

BASE="http://search.maven.org/remotecontent?filepath="

msg () {
    echo "$@" >&2
}

download () {
    DESTDIR="$1"
    PKG="$2"
    VERSION="$3"
    PREFIX="com/amazonaws/$PKG/$VERSION/$PKG-$VERSION"
    TYPES="pom jar"
    mkdir -p "$DESTDIR"

    for TYPE in $TYPES; do
        URL="${BASE}${PREFIX}.${TYPE}"
        FILE="$DESTDIR/$(basename $URL)"
        if [ ! -r "$FILE" ]; then
            msg "downloading $URL to $FILE"
            curl -L -o "$FILE" "$URL"
        fi
    done
    POM="$DESTDIR/$PKG-$VERSION.pom"
    mvn -B -f "$POM" dependency:copy-dependencies -DoutputDirectory="$DESTDIR"
}

download "$DYNAMO_DESTDIR" "$DYNAMO_PKG" "$DYNAMO_VERSION"
download "$KCL_DESTDIR" "$KCL_PKG" "$KCL_VERSION"

echo "done"
