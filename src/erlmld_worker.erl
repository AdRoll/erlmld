%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%    KCL MultiLangDaemon worker (record processor) behavior.
%%%
%%%    A worker has the following lifecycle:
%%%
%%%        INITIALIZE -> PROCESSING -> SHUTDOWN
%%%
%%%    When a shard lease has been obtained, a worker is initialized to process records
%%%    appearing on that shard.  It is provided the opaque data which was supplied to
%%%    erlmld_sup, the shard name, and initial sequence number(s) (may be undefined for
%%%    new shards or if using V1 protocol), and returns an opaque worker_state() value
%%%    which is passed to process_records/2 and shutdown/2.
%%%
%%%    As records are read from the stream, they are b64decoded and passed to
%%%    process_record/2.  If a record was put on the stream using KPL aggregation, it is
%%%    also deaggregated, with each sub-record provided to the worker as a single record
%%%    along with a subsequence number.
%%%
%%%    After processing each record, a worker returns an updated worker_state().  It may
%%%    also return a checkpoint() (not necessarily the latest) containing a
%%%    sequence_number() from that record or a previous record, which will result in an
%%%    attempt to checkpoint the stream at the associated sequence number.  If the
%%%    supplied checkpoint() has an undefined sequence number, the stream is checkpointed
%%%    at the most recent sequence number.
%%%
%%%    Before starting to process each batch of records, a worker's ready/1 callback is
%%%    called, which should return a possibly-updated worker state and possibly a
%%%    checkpoint.  This can be useful when a record processor is using a watchdog timer
%%%    and is far behind on a stream (and so won't receive any actual records for a
%%%    while), or if a stream has very low volume (records seen less frequently than
%%%    desired checkpoint or flush intervals).
%%%
%%%    When a shard lease has been lost or a shard has been completely processed, a worker
%%%    will be shut down.  If the lease was lost, the worker will receive a reason of
%%%    'zombie', and it should not checkpoint (and any checkpoint response is in error).
%%%    If the shard was closed, the reason will be 'terminate' and the worker should
%%%    return a checkpoint response.  That checkpoint should either have an undefined
%%%    sequence number, or it should be the most recent sequence number which was provided
%%%    to process_record/2.
%%%
%%%    If a worker returns an error response, it is fatal.
%%%
%%%    See also: https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/MultiLangProtocol.java
%%%
%%% @end
%%% Created : 18 Nov 2016 by Mike Watters <mike.watters@adroll.com>

-module(erlmld_worker).

-include("erlmld.hrl").


-callback initialize(term(), shard_id(), sequence_number() | undefined) ->
    {ok, worker_state()}
        | {error, term()}.

-callback ready(worker_state()) ->
    {ok, worker_state()}
        | {ok, worker_state(), checkpoint()}
        | {error, term()}.

-callback process_record(worker_state(), stream_record()) ->
    {ok, worker_state()}
        | {ok, worker_state(), checkpoint()}
        | {error, term()}.

-callback checkpointed(worker_state(), sequence_number(), checkpoint()) ->
    {ok, worker_state()}
        | {error, term()}.

-callback shutdown(worker_state(), shutdown_reason()) ->
    ok
        | {ok, checkpoint()}
        | {error, term()}.
