#!/bin/sh
#
#    This script is run by the MultiLangDaemon as a worker subprocess which processes
#    records on a Kinesis/DynamoDB stream shard.  The script expects one argument, a port
#    number to connect to using TCP.  The expectation is that it will be used with a
#    program like socat or netcat, mapping stdin/stdout to a tcp socket on which erlmld is
#    running the MLD protocol.  The script and its argument are specified in a .properties
#    file which is loaded by the MLD.
#
set -eu

msg () {
    echo "$@" >&2
}

if [ "$#" -lt 1 ] || [ -z "$1" ]; then
    msg "usage: $0 listen-port"
    exit 1
fi

HOST="localhost"
PORT="$1"

WAIT_ARG=""
if [ ! -z "${DEBUG:-""}" ]; then
    LOCKFILE="${TMPDIR:-/tmp}/$(basename $0).$HOST.$PORT.lock"
    WAIT_ARG="-W$LOCKFILE"
    msg "DEBUG set, using lockfile $LOCKFILE"
else
    # sleep for a random amount of time (0-30s) to stagger startup:
    sleep $(od -A n -N 4 -t u4 /dev/urandom | head -1 | awk '{print int($1)%31}')
fi

exec socat $WAIT_ARG - TCP4:$HOST:$PORT
