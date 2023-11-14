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
DYNAMO_VERSION="1.5.3"
DYNAMO_DESTDIR="$R/ddb_jars"

KCL_PKG="amazon-kinesis-client"
KCL_VERSION="1.14.5"
KCL_DESTDIR="$R/kcl_jars"

BASE="http://search.maven.org/remotecontent?filepath="

msg () {
    echo "$@" >&2
}

maybe_fix_dynamodblocal_dep_in_pom () {
    POM_PATH="$1"
    PKG_NAME="$2"
    PKG_VSN="$3"

    # "dynamodb-streams-kinesis-adapter" includes in its POM a test-ONLY dependency
    # called "DynamoDBLocal". At the moment of writing (2023-10-27), the adapter
    # release we are using (1.5.3), which was released in May 11, 2021, states that
    # the test dependency should be downloaded in a version matching "[1.12,2.0)"
    # (i.e., 1.12 <= version < 2.0), and AWS did some recent releases of this testing
    # libary in 2023, with the latest version (1.24.0) having being published on
    # October 26, 2023, as per what its release history says.
    # However, this release is missing in the Maven repository, with the latest
    # version meeting the release requirement being 1.23.0. The fun thing is that nonetheless,
    # we have seen in our CI maven try and install the latest yet-to-be-uploaded 1.24.0 release.
    # This means that the download will fail, and for now the fix we have come up with is
    # changing the version constraints for "DynamoDBLocal" from a range to a pinned 1.23.0 release.
    #
    # References:
    #   - https://mvnrepository.com/artifact/com.amazonaws/dynamodb-streams-kinesis-adapter/1.5.3
    #   - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocalHistory.html
    #   - https://mvnrepository.com/artifact/com.amazonaws/DynamoDBLocal
    if [[ $PKG_NAME == $DYNAMO_PKG ]] && [[ $PKG_VSN == $DYNAMO_VERSION ]]; then
        msg "sed'ing the pom for $PKG_NAME ($PKG_VSN) found in $POM_PATH"

        UNAME_SYS=$(uname -s)
        if [[ $UNAME_SYS == "Darwin" ]]; then
            # macOS sed syntax
            sed -i '' 's/<aws.dynamodblocal.version>\[1.12,2.0)/<aws.dynamodblocal.version>1.23.0/' $POM_PATH
        else
            # Linux sed syntax
            sed -i 's/<aws.dynamodblocal.version>\[1.12,2.0)/<aws.dynamodblocal.version>1.23.0/' $POM_PATH
        fi
    fi
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
    maybe_fix_dynamodblocal_dep_in_pom "$POM" "$DYNAMO_PKG" "$DYNAMO_VERSION"
    mvn -B -f "$POM" dependency:copy-dependencies -DoutputDirectory="$DESTDIR" -DincludeScope=runtime
}

download "$DYNAMO_DESTDIR" "$DYNAMO_PKG" "$DYNAMO_VERSION"
download "$KCL_DESTDIR" "$KCL_PKG" "$KCL_VERSION"

echo "done"
