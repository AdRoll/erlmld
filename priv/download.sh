#!/bin/bash
#
#    Download the latest released AWS DynamoDB streams adapter and all required
#    dependencies (one of which is the KCL) to ./jars.  Requires maven.
#
set -eu

DYNAMO_PKG="dynamodb-streams-kinesis-adapter"
DYNAMO_VERSION="1.2.1"

msg () {
    echo "$@" >&2
}

cd "$(dirname $0)"
R="$(pwd)"
DESTDIR="$R/jars"
mkdir -p "$DESTDIR"

BASE="http://search.maven.org/remotecontent?filepath="

download () {
    PKG="$1"
    VERSION="$2"
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

download "$DYNAMO_PKG" "$DYNAMO_VERSION"

echo "done"
