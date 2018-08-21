#!/bin/bash
#
#    Run the MultiLangDaemon using the properties file supplied as the single argument to
#    this script.  The directory containing the properties file is added to the classpath,
#    and it is referenced by filename.  The directory containing this script is appended
#    to PATH.
#
#    The "./kcl_jars" directory relative to this script should have been populated by
#    $0/download.sh.
#
set -euo pipefail

if [ ! -r "${1:-}" ]; then
    echo "usage: $0 file.properties" >&2
    exit 1
fi

R="$(dirname $0)"
JAVA="${JAVA:-${JAVA_HOME:-/usr}/bin/java}"

CLASS=com.amazonaws.services.kinesis.multilang.MultiLangDaemon

export PATH="$PATH:$R"

"$JAVA" -cp "$R/kcl_jars/*":"$(dirname $1)" $CLASS "$(basename $1)"
