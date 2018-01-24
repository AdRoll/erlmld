%%%-------------------------------------------------------------------
%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%     TCP protocol handler implementing the MultiLangDaemon protocol (V1 and V2).
%%%
%%%     The MultiLangDaemon protocol is a line-based request-response protocol with a JSON
%%%     transport encoding and no pipelining.  Requests contain a single newline.  Each
%%%     request must have a response.
%%%
%%%     Requests are of the form:
%%%
%%%         { "action": ACTION-NAME, ... action-specific data } \n
%%%
%%%     Responses are of the form:
%%%
%%%         JSON-RESPONSE \n
%%%
%%%
%%%     MLD V1 requests:
%%%
%%%         { "action": "initialize", "shardId": SHARD-ID }
%%%
%%%         { "action": "shutdown", "reason": REASON }
%%%
%%%         {  "action": "processRecords",
%%%           "records": [{           "data": B64-DATA,      % may contain KPL aggregate record data
%%%                           "partitionKey": PARTITION-KEY,
%%%                         "sequenceNumber": SEQNO }]
%%%         }
%%%
%%%
%%%     MLD V2 requests:
%%%
%%%         {           "action": "initialize",
%%%                    "shardId": SHARD-ID,
%%%             "sequenceNumber": SEQNO,
%%%          "subSequenceNumber": SUBSEQNO
%%%         }
%%%
%%%         { "action": "shutdown", "reason": REASON }
%%%
%%%         {             "action": "processRecords",
%%%           "millisBehindLatest": DELAY-MS,
%%%                      "records": [{            "action": "record",
%%%                                                 "data": B64-DATA,      % does not contain KPL aggr record data
%%%                                         "partitionKey": PARTITION-KEY,
%%%                          "approximateArrivalTimestamp": WRITE-TIME-MS  % epoch utc ms when written to stream
%%%                                       "sequenceNumber": SEQNO,
%%%                                    "subSequenceNumber": SUBSEQNO }]
%%%         }
%%%
%%%         { "action": "shutdownRequested" }
%%%
%%%
%%%     Worker responses:
%%%
%%%         { "action": "status", "responseFor": REQUEST-ACTION-NAME }
%%%
%%%
%%%     Worker V1 requests:
%%%
%%%         { "action": "checkpoint", "checkpoint": SEQNO }
%%%
%%%     Worker V2 requests:
%%%
%%%         {            "action": "checkpoint",
%%%              "sequenceNumber": SEQNO,
%%%           "subSequenceNumber": SUBSEQNO
%%%         }
%%%
%%%
%%%     MLD V1 responses:
%%%
%%%         { "action": "checkpoint", "sequenceNumber": SEQNO, "error": ERROR }
%%%
%%%     MLD V2 responses:
%%%
%%%         { "action": "checkpoint", "sequenceNumber": SEQNO, "subSequenceNumber": SUBSEQNO, "error": ERROR }
%%%
%%%
%%%     If checkpointing succeeded, ERROR will be null or absent, and the sequence number(s)
%%%     will indicate which sequence/subsequence number was checkpointed.  If
%%%     checkpointing fails, a worker should probably exit.
%%%
%%%     Notes:
%%%
%%%      - The MLD ignores blank lines (accepts \n JSON \n), and so do we.
%%%
%%%      - There is no way to indicate failure except by a worker exiting.  The MLD
%%%      doesn't currently handle worker failure, so if we exit, the MLD and all of its
%%%      workers will die.
%%%
%%%      - We only emit checkpoint requests during processing of other requests, and
%%%      before returning our responses for those requests.  E.G., after a
%%%      "processRecords" request or a non-ZOMBIE "shutdown" request, before returning the
%%%      "status" response.
%%%
%%%     See also: https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/MultiLangRecordProcessor.java
%%%
%%% @end
%%% Created : 18 Nov 2016 by Mike Watters <mike.watters@adroll.com>
%%%-------------------------------------------------------------------
-module(erlmld_wrk_statem).

-behaviour(gen_statem).

%% API
-export([start_link/2, accept/2]).

%% gen_statem callbacks
-export([init/1, callback_mode/0, terminate/3, code_change/4, handle_event/4]).

-record(data, {
          %% name of handler module implementing erlmld_worker behavior:
          handler_module :: atom(),

          %% opaque term used as first argument to handler_module's initialize/3:
          handler_data :: term(),

          %% connected socket owned by this process:
          socket :: gen_tcp:socket(),

          %% input buffer;  responses are small and we need no output buffer:
          buf = [] :: list(binary()),

          %% worker state returned from handler module init:
          worker_state :: term(),

          %% if true, the MLD made a processRecords call with the V2 format (supplied
          %% millisBehindLatest), so we will checkpoint using the V2 checkpoint format:
          is_v2 = false :: boolean(),

          %% most recent action name from the peer:
          last_request :: binary(),

          %% last attempted checkpoint:
          last_checkpoint :: checkpoint()
         }).

-define(INTERNAL, internal).

%% state atoms.  some states are 2-tuples of these atoms.
-define(INIT, init).
-define(SHUTDOWN, shutdown).
-define(PEER_READ, peer_read).
-define(REQUEST, request).
-define(CHECKPOINT, checkpoint).
-define(SHUTDOWN_CHECKPOINT, shutdown_checkpoint).
-define(DISPATCH, dispatch).
-define(PROCESS_RECORDS, process_records).
-define(PEER_WRITE, peer_write).
-define(ABORT, abort).

-define(RETRY_SLEEP, application:get_env(erlmld, checkpoint_retry_sleep, 10000)).

-include("erlmld.hrl").
-include("kpl_agg_pb.hrl").

%%%===================================================================
%%% API
%%%===================================================================

start_link(HandlerMod, HandlerData) ->
    gen_statem:start_link(?MODULE, [HandlerMod, HandlerData], []).

accept(Pid, Socket) ->
    ok = gen_tcp:controlling_process(Socket, Pid),
    gen_statem:call(Pid, {accepted, Socket}).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() ->
    handle_event_function.

init([HandlerMod, HandlerData]) ->
    {ok, ?INIT, #data{handler_module = HandlerMod,
                      handler_data = HandlerData}}.

terminate(_Reason, _State, #data{socket = Socket}) ->
    gen_tcp:close(Socket),
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.


%% a connection has been accepted.  start reading requests from it.
handle_event({call, From}, {accepted, Socket}, ?INIT, Data) ->
    {next_state, {?PEER_READ, ?REQUEST}, activate(Data#data{socket = Socket}),
     [{reply, From, ok}]};


%% connection was closed, and we expected it to be closed.  this will occur when the MLD
%% has instructed us to shut down, and we've returned a "success" response for the
%% shutdown action (MLD will read our response, then close its stream to us, then await
%% our exit).
handle_event(info, {tcp_closed, _}, {?PEER_READ, ?SHUTDOWN}, _) ->
    stop;

%% connection was closed, but we didn't expect it.
handle_event(info, {tcp_closed, _}, _State, _) ->
    {stop, {error, unexpected_close}};


%% we are trying to read some data, and some data has been received.  if we've received a
%% complete line (action), we dispatch it to be handled according to its kind (i.e., a new
%% request or a response to a checkpoint attempt).
handle_event(info, {tcp, Socket, Bin}, {?PEER_READ, NextReadKind}, Data) ->
    case next_action(Bin, Data) of
        {undefined, NData, <<>>} ->
            {keep_state, activate(NData)};

        {{error, _} = Error, _, _} ->
            {stop, Error};

        {#{<<"action">> := Action} = ActionData, NData, Rest} ->
            {next_state, {?DISPATCH, NextReadKind},
             case NextReadKind of
                 ?REQUEST -> NData#data{last_request = Action};
                 _ -> NData
             end,
             [{next_event, ?INTERNAL, ActionData}
              | case Rest of
                    <<>> -> [];
                    _ -> [{next_event, info, {tcp, Socket, Rest}}]
                end]}
    end;


%% MLD is initializing us, and we haven't been initialized yet.
handle_event(?INTERNAL, #{<<"action">> := <<"initialize">>,
                          <<"shardId">> := ShardId} = R,
             {?DISPATCH, ?REQUEST},
             #data{handler_module = HandlerMod,
                   handler_data = HandlerData,
                   worker_state = undefined} = Data) ->
    ISN = sequence_number(R),
    case HandlerMod:initialize(HandlerData, ShardId, ISN) of
        {ok, WorkerState} ->
            success(R, worker_state(Data, WorkerState), ?REQUEST);

        {error, _} = Error ->
            {stop, Error}
    end;


%% MLD is instructing us to shut down after we've been initialized.  if the reason is
%% TERMINATE, we should finish processing and checkpoint (shard is being closed).  if the
%% reason is not TERMINATE (i.e., ZOMBIE), we should abort any outstanding work and stop
%% without checkpointing.  in both cases, we should return a success response and stop
%% (resulting in connection closure).
handle_event(?INTERNAL, #{<<"action">> := <<"shutdown">>,
                          <<"reason">> := ReasonBin} = R,
             {?DISPATCH, State},
             #data{handler_module = Mod,
                   worker_state = {ok, WorkerState}} = Data) when State == ?REQUEST;
                                                                  State == ?SHUTDOWN ->
    Reason = case ReasonBin of
                 <<"TERMINATE">> -> terminate;
                 <<"ZOMBIE">> -> zombie;
                 _ -> unknown
             end,
    case {Reason, Mod:shutdown(WorkerState, Reason)} of
        {terminate, {ok, Checkpoint}} ->
            %% shard terminating.  worker supplied a checkpoint.  checkpoint and then
            %% return a success response for the shutdown if the checkpoint was
            %% successful.
            shutdown_checkpoint(Data, Checkpoint);

        {terminate, ok} ->
            %% worker should have checkpointed for this TERMINATE shutdown.
            error_logger:error_msg("~p did not checkpoint after TERMINATE~n", [WorkerState]),
            {stop, {error, expected_checkpoint}};

        {_, ok} ->
            %% non-terminate shutdown, worker did not checkpoint; normal exit.
            success(R, Data, ?SHUTDOWN);

        {_, {ok, _Checkpoint}} ->
            %% worker should only checkpoint during shutdown for a TERMINATE shutdown.
            error_logger:error_msg("~p attempted to checkpoint during ~p shutdown~n",
                                   [WorkerState, ReasonBin]),
            {stop, {error, unexpected_checkpoint}};

        {_, {error, _} = Error} ->
            {stop, Error}
    end;


%% MLD is gracefully stopping all workers, which can elect to checkpoint or not.  right
%% now we treat this the same way as a "zombie" shutdown (lost lease).  need to add a
%% non-zombie, non-terminate reason to worker behavior so we can support checkpointing
%% here.
handle_event(?INTERNAL, #{<<"action">> := <<"shutdownRequested">>} = R,
             {?DISPATCH, ?REQUEST},
             #data{handler_module = Mod,
                   worker_state = WS} = Data) ->
    case WS of
        {ok, WorkerState} ->
            case Mod:shutdown(WorkerState, zombie) of
                ok ->
                    %% non-terminate shutdown, worker did not checkpoint; normal exit.
                    success(R, Data, ?SHUTDOWN);

                {ok, _Checkpoint} ->
                    %% worker should only checkpoint during shutdown for a TERMINATE shutdown;
                    %% this isn't supported yet.
                    error_logger:error_msg("~p attempted to checkpoint during shutdownRequest shutdown~n",
                                           [WorkerState]),
                    {stop, {error, unexpected_checkpoint}};

                {_, {error, _} = Error} ->
                    {stop, Error}
            end;
        _ ->
            %% not initialized yet.
            success(R, Data, ?SHUTDOWN)
    end;


%% MLD is providing some records to be processed.  We will call the worker's ready/1
%% callback, then deaggregate them all at once if they are in KPL format, and then provide
%% each in turn to the handler module, which will have the opportunity to checkpoint after
%% each record.  the MLD should wait for our "status" response before sending any
%% additional records or other requests.
handle_event(?INTERNAL, #{<<"action">> := <<"processRecords">>,
                          <<"records">> := Records} = R,
             {?DISPATCH, ?REQUEST},
             #data{handler_module = Mod,
                   worker_state = {ok, WorkerState}} = Data) ->
    case Mod:ready(WorkerState) of
        {ok, NWorkerState} ->
            NData = worker_state(Data#data{is_v2 = maps:is_key(<<"millisBehindLatest">>, R)},
                                 NWorkerState),
            process_records(R, NData, deaggregate_kpl_records(R, Records));

        {error, _} = Error ->
            {stop, Error}
    end;


%% MLD is returning a checkpoint response.  if a fatal error occurred, we shouldn't
%% process any more data and should exit.  with the current version of the MLD, this will
%% cause all other workers managed by the MLD to be killed (because the MLD will exit).
%%
%% if the checkpoint was successful, we resume processing records (if we were processing
%% them; a {process, ...} event will be pending, which will be retried after we change
%% state).  if we are instead shutting down, we continue the shutdown process.
%%
%% the following errors are handled; all other checkpoint errors are fatal:
%%
%%   ShutdownException:
%%
%%     another worker stole our lease while we were processing or shutting down.  ignore
%%     error and abort record processing or continue shutdown as appropriate.
%%
%%   ThrottlingException:
%%
%%     MLD was throttled when performing a checkpoint.  we retry forever after a random
%%     sleep.
%%
handle_event(?INTERNAL, #{<<"action">> := <<"checkpoint">>,
                          <<"error">> := <<"ShutdownException">>},
             {?DISPATCH, ?CHECKPOINT},
             #data{worker_state = {ok, WorkerState}} = Data) ->
    %% we were processing records and we attempted to checkpoint, but another worker stole
    %% our lease.  abort processing.  we should receive a ZOMBIE shutdown reason and exit
    %% normally.
    error_logger:warning_msg("~p received shutdown exception during checkpoint; aborting processing~n",
                             [WorkerState]),
    {next_state, ?ABORT, Data};

handle_event(?INTERNAL, #{<<"action">> := <<"checkpoint">>,
                          <<"error">> := <<"ShutdownException">>},
             {?DISPATCH, ?SHUTDOWN_CHECKPOINT},
             #data{worker_state = {ok, WorkerState},
                   last_request = LastAction} = Data) ->
    %% we were shutting down and attempted to checkpoint, but another worker stole our
    %% lease during the shutdown.  return a 'success' response to the shutdown command.
    %% we should exit normally.
    error_logger:warning_msg("~p received shutdown exception during shutdown checkpoint~n",
                             [WorkerState]),
    success(LastAction, Data, ?SHUTDOWN);

handle_event(?INTERNAL, #{<<"action">> := <<"checkpoint">>,
                          <<"error">> := <<"ThrottlingException">>},
             {?DISPATCH, CheckpointState},
             #data{worker_state = {ok, WorkerState},
                   last_checkpoint = Checkpoint} = Data)
  when CheckpointState == ?CHECKPOINT;
       CheckpointState == ?SHUTDOWN_CHECKPOINT ->
    %% we attempted to checkpoint, but were throttled.  this may mean the dynamodb state
    %% table used by the KCL is underprovisioned, or workers are checkpointing in
    %% lockstep.  retry forever after a random sleep of up to ?RETRY_SLEEP ms.
    error_logger:warning_msg("~p throttled during ~p; consider increasing state table write capacity~n",
                             [WorkerState, CheckpointState]),
    timer:sleep(rand:uniform(?RETRY_SLEEP)),
    do_checkpoint(Data, Checkpoint, CheckpointState, []);

handle_event(?INTERNAL, #{<<"action">> := <<"checkpoint">>,
                          <<"error">> := CheckpointError},
             {?DISPATCH, CheckpointState},
             _Data) when (CheckpointState == ?CHECKPOINT orelse
                          CheckpointState == ?SHUTDOWN_CHECKPOINT),
                         CheckpointError /= undefined ->
    {stop, {error, {checkpoint_failure, CheckpointError}}};

handle_event(?INTERNAL, #{<<"action">> := <<"checkpoint">>} = R,
             {?DISPATCH, CheckpointState},
             #data{worker_state = {ok, WorkerState},
                   last_checkpoint = Checkpoint,
                   last_request = LastAction} = Data)
  when CheckpointState == ?CHECKPOINT;
       CheckpointState == ?SHUTDOWN_CHECKPOINT ->
    %% successful checkpoint. fixme; provide indication of success to worker?
    SN = sequence_number(R),
    error_logger:info_msg("~p checkpointed at ~p (~p)~n", [WorkerState, Checkpoint, SN]),
    case CheckpointState of
        ?CHECKPOINT ->
            {next_state, ?PROCESS_RECORDS, Data};
        ?SHUTDOWN_CHECKPOINT ->
            success(LastAction, Data, ?SHUTDOWN)
    end;


%% we were processing records and we attempted to checkpoint, but failed because another
%% worker stole our lease.  abort record processing, return a 'success' response for
%% the current command, and read the next request, which should be a shutdown command.
handle_event(?INTERNAL, {process, R, _}, ?ABORT, Data) ->
    success(R, Data, ?REQUEST);

%% we were processing records, and have now processed them all.  return a 'success'
%% response for the current command, and read the next command.
handle_event(?INTERNAL, {process, R, []}, ?PROCESS_RECORDS, Data) ->
    success(R, Data, ?REQUEST);

%% we're processing records.  process the next record, update the worker state according
%% to the return value, and possibly perform a checkpoint if desired before processing any
%% subsequent records.
handle_event(?INTERNAL, {process, R, [Record | Records]},
             ?PROCESS_RECORDS,
             #data{handler_module = Mod,
                   worker_state = {ok, WorkerState}} = Data) ->
    case Mod:process_record(WorkerState, Record) of
        {ok, NWorkerState} ->
            process_records(R, worker_state(Data, NWorkerState), Records);

        {ok, NWorkerState, Checkpoint} ->
            checkpoint(R, worker_state(Data, NWorkerState), Checkpoint, Records);

        {error, _} = Error ->
            {stop, Error}
    end;

%% we suspend record processing while awaiting a checkpoint response.  this event will be
%% seen again after changing states.
handle_event(?INTERNAL, {process, _, _}, {?PEER_READ, ?CHECKPOINT}, _) ->
    {keep_state_and_data, postpone};


%% we've been given something (json data iolist) to write; we write it and then read &
%% handle the next action according to the specified read kind.  we expect data we write
%% to be small (i.e., action responses and checkpoint requests).  while waiting for the
%% next action of kind NextReadKind, we do nothing.
handle_event(?INTERNAL, {write, IoData}, {?PEER_WRITE, NextReadKind}, #data{socket = Socket,
                                                                            buf = Buf} = Data) ->
    %% there should be no unprocessed/buffered request data at this point:
    case Buf of
        [] -> ok;
        _ ->
            throw({stop, {error, {unprocessed_request_data, Buf}}})
    end,
    ok = gen_tcp:send(Socket, [IoData, "\n"]),
    {next_state, {?PEER_READ, NextReadKind}, activate(Data)};


%% some tcp data was received, but couldn't be handled in the current state.  this event
%% will be seen again after changing states.
handle_event(info, {tcp, _Socket, _Bin}, _State, _Data) ->
    {keep_state_and_data, postpone};


handle_event(info, Message, _State, #data{worker_state = WorkerState}) ->
    error_logger:error_msg("~p ignoring unexpected message ~p~n", [WorkerState, Message]),
    keep_state_and_data.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% activate the socket once, causing another tcp message to be sent to us.
activate(#data{socket = Socket} = Data) ->
    inet:setopts(Socket, [{active, once}]),
    Data.


%% given an input (request) action (a map) or an action name, cause a "success" response
%% for that action to be written, changing state according to NextState after writing.
success(#{<<"action">> := Action}, Data, NextState) ->
    success(Action, Data, NextState);
success(Action, Data, NextState) when is_binary(Action) ->
    IoData = jiffy:encode(#{<<"action">> => <<"status">>,
                            <<"responseFor">> => Action}),
    {next_state, {?PEER_WRITE, NextState}, Data,
     [{next_event, ?INTERNAL, {write, IoData}}]}.


%% given a list of records, change state if needed, and send a single {process, ...} event
%% with those records.
process_records(R, Data, Records) ->
    {next_state, ?PROCESS_RECORDS, Data,
     [{next_event, ?INTERNAL, {process, R, Records}}]}.


%% checkpoint, then continue processing the remaining records after receiving the
%% checkpoint response.
checkpoint(R, Data, Checkpoint, Records) ->
    do_checkpoint(Data, Checkpoint, ?CHECKPOINT,
                  [{next_event, ?INTERNAL, {process, R, Records}}]).

%% checkpoint, then shutdown after receiving the checkpoint response.
shutdown_checkpoint(Data, Checkpoint) ->
    do_checkpoint(Data, Checkpoint, ?SHUTDOWN_CHECKPOINT, []).


%% cause an attempt to checkpoint at the given sequence number, or the latest sequence
%% number seen by the MLD on the stream if undefined.  then continue processing according
%% to the supplied next state, also appending the given events.
do_checkpoint(Data, #checkpoint{sequence_number = SN} = Checkpoint, NextState, NextEvents) ->
    Enc = jiffy:encode(maps:put(<<"action">>, <<"checkpoint">>, checkpoint_spec(Data, SN))),
    {next_state, {?PEER_WRITE, NextState}, Data#data{last_checkpoint = Checkpoint},
     [{next_event, ?INTERNAL, {write, Enc}} | NextEvents]}.


checkpoint_spec(#data{is_v2 = true}, #sequence_number{base = Base, sub = Sub}) ->
    #{<<"sequenceNumber">> => encode_seqno_base(Base),
      <<"subSequenceNumber">> => case Sub of
                                     undefined -> null;
                                     _ -> Sub
                                 end};
checkpoint_spec(#data{is_v2 = true}, undefined) ->
    #{<<"sequenceNumber">> => null,
      <<"subSequenceNumber">> => null};

checkpoint_spec(#data{is_v2 = false}, #sequence_number{base = Base}) ->
    #{<<"checkpoint">> => encode_seqno_base(Base)};
checkpoint_spec(#data{is_v2 = false}, undefined) ->
    #{<<"checkpoint">> => null}.


%% given a binary, strip the last byte if it is a carriage return.
-spec strip_cr(binary()) -> binary().
strip_cr(<<>>) ->
    <<>>;
strip_cr(Bin) ->
    case [binary:at(Bin, size(Bin) - 1)] of
        "\r" ->
            binary:part(Bin, {0, size(Bin) - 1});
        _ ->
            Bin
    end.


%% given a binary corresponding to some input data and a #data{}, return a 3-tuple of the
%% next line (if any), next state, and remaining data.  if the input binary contains a
%% newline, return as the first element a binary consisting of the already-buffered data
%% followed by that binary.  otherwise, add it to the buffer.  the buffer never contains
%% newlines.
-spec next_line(binary(), #data{}) -> {binary() | undefined, #data{}, binary()}.
next_line(Bin, #data{buf = Buf} = Data) ->
    case binary:split(Bin, <<"\n">>) of
        [_] ->
            {undefined, Data#data{buf = [Bin | Buf]}, <<>>};

        [EOL, Rest] ->
            Line = iolist_to_binary(lists:reverse([strip_cr(EOL) | Buf])),
            {Line, Data#data{buf = []}, Rest}
    end.


%% given a binary and #data{}, return a 3-tuple of the next action (if any), next state,
%% and remaining data.  an "action" is a line which should have been a json-encoded map
%% containing an "action" key.  if decoding fails with a thrown error, that error is
%% returned as the decoded value.
-spec next_action(binary(), #data{}) -> {map() | undefined, #data{}, binary()}.
next_action(Bin, Data) ->
    case next_line(Bin, Data) of
        {undefined, NData, Rest} ->
            {undefined, NData, Rest};

        {<<>>, NData, Rest} ->
            {undefined, NData, Rest};

        {Line, NData, Rest} ->
            Dec = try
                      jiffy:decode(Line, [return_maps,
                                          {null_term, undefined}])
                  catch
                      throw:{error, Error} ->
                          {error, Error}
                  end,
            {Dec, NData, Rest}
    end.


%% given a value which is possibly a map containing a `sequenceNumber` key with a binary
%% string value possibly denoting an integer, and possibly a `subSequenceNumber` key with
%% an integer value, return a sequence_number() corresponding to those values, otherwise
%% undefined.
sequence_number(#{<<"sequenceNumber">> := SN} = M) when is_binary(SN) ->
    SubSeq = maps:get(<<"subSequenceNumber">>, M, undefined),
    #sequence_number{base = decode_seqno_base(SN),
                     sub = SubSeq,
                     user_sub = SubSeq};

sequence_number(#{<<"sequenceNumber">> := undefined}) ->
    #sequence_number{};

sequence_number(_) ->
    undefined.

% A version that takes the sequence numbers as args directly.
sequence_number(SN, OriSSN, SSN, Total) when is_binary(SN) ->
    #sequence_number{base = decode_seqno_base(SN), sub = OriSSN, user_sub = SSN, user_total = Total};

sequence_number(undefined, _, _, _) ->
    undefined.

non_kpl_sequence_number(#{<<"sequenceNumber">> := SN} = M) when is_binary(SN) ->
    SNRecord = sequence_number(M),
    SNRecord#sequence_number{user_sub = undefined}.

partition_key(#{<<"partitionKey">> := PK} = _Record) ->
    PK.


worker_state(Data, WorkerState) ->
    Data#data{worker_state = {ok, WorkerState}}.


stream_record(Record, PartitionKey, Data, SequenceNumber) ->
    {Timestamp, Delay} = case Record of
                             #{<<"action">> := <<"record">>} ->
                                 {maps:get(<<"approximateArrivalTimestamp">>, Record, undefined),
                                  maps:get(<<"millisBehindLatest">>, Record, undefined)};
                             _ ->
                                 {undefined, undefined}
                         end,
    #stream_record{
        partition_key = PartitionKey,
        data = Data,
        sequence_number = SequenceNumber,
        timestamp = Timestamp,
        delay = Delay
    }.


deaggregate_kpl_records(R, Records) ->
    lists:flatmap(fun (#{<<"data">> := RecordData} = Record) ->
                          %% note: b64fast will fail if the data contains whitespace.
                          deaggregate_kpl_record(R, Record, b64fast:decode64(RecordData))
                  end, Records).


deaggregate_kpl_record(_R, Record, <<Magic:4/binary, Data/binary>> = _DecodedData)
  when Magic == ?KPL_AGG_MAGIC;
       Magic == ?KPL_AGG_MAGIC_DEFLATED ->
    %% This is an aggregated/deflated V1 record, or a deflated aggregated V2 record.
    %% See https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
    %% fixme; move this to kpl_agg.  also, make user-supplied decode functions configurable
    AggData = case Magic of
                  ?KPL_AGG_MAGIC_DEFLATED ->
                    zlib:uncompress(Data);
                _ ->
                    Data
              end,
    L = byte_size(AggData),
    ProtoMsg = binary:part(AggData, 0, L - 16),
    Checksum = binary:part(AggData, L, -16),
    check_md5(ProtoMsg, Checksum),
    decode_kpl_protobuf_message(Record, ProtoMsg);

deaggregate_kpl_record(R, Record, DecodedData) ->
    %% This is a non-aggregated record.
    [stream_record(R, partition_key(Record), DecodedData, non_kpl_sequence_number(Record))].


check_md5(Data, Checksum) ->
    case application:get_env(erlmld, check_md5_of_agg_data, true) of
        true ->
            case crypto:hash(md5, Data) of
                Checksum -> ok;
                _ -> error("md5 checksum failed for aggregated record")
            end;
        _ -> ok
    end.


decode_kpl_protobuf_message(#{<<"sequenceNumber">> := SN} = AggRecord, ProtoMsg) ->
    #'AggregatedRecord'{partition_key_table = PKsList,
                        records = Records} = kpl_agg_pb:decode_msg(ProtoMsg, 'AggregatedRecord'),
    PKs = list_to_tuple(PKsList),
    SubSeq = maps:get(<<"subSequenceNumber">>, AggRecord, undefined),
    [stream_record(AggRecord, element(PKIndex + 1, PKs), Data, sequence_number(SN, SubSeq, SSN, length(Records)))
        || {#'Record'{partition_key_index = PKIndex, data = Data} = _Record, SSN}
            <- lists:zip(Records, lists:seq(0, length(Records) - 1))].



%% decode a sequence number string to an integer if possible, or an atom.  this is done to
%% save space.
decode_seqno_base(<<C, _/binary>> = Bin)
  when C >= $0, C =< $9 ->
    binary_to_integer(Bin);
decode_seqno_base(Bin) ->
    %% note: the set of values which can be taken is small.
    binary_to_atom(Bin, utf8).

encode_seqno_base(X) when is_integer(X) ->
    integer_to_binary(X);
encode_seqno_base(X) when is_atom(X), X /= undefined ->
    atom_to_binary(X, utf8).


%%%===================================================================
%%% TESTS
%%%===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


deaggregate_kpl_record_v1_noagg_test() ->
    %% Test a non-aggregated record.
    R = #{},
    NonAggRecord = #{<<"partitionKey">> => <<"PK">>, <<"sequenceNumber">> => <<"666">>},
    NonAggData = <<"whatever">>,
    [Result] = deaggregate_kpl_record(R, NonAggRecord, NonAggData),
    ?assertEqual(<<"PK">>, Result#stream_record.partition_key),
    ?assertEqual(<<"whatever">>, Result#stream_record.data),
    ?assertEqual(666, Result#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(undefined, Result#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(undefined, Result#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(undefined, Result#stream_record.sequence_number#sequence_number.user_total),
    ok.


deaggregate_kpl_record_v1_agg_test() ->
    %% Test an aggregated record.
    %% The data is the same as in kpl_agg:simple_aggregation_test.
    R = #{},
    AggRecord = #{<<"partitionKey">> => <<"ignored">>, <<"sequenceNumber">> => <<"666">>, <<"subSequenceNumber">> => 0},
    AggData = <<(?KPL_AGG_MAGIC)/binary,10,3,112,107,49,10,3,112,107,50,18,4,101,104,
                107,49,18,4,101,104,107,50,26,11,8,0,16,0,26,5,100,97,116,97,
                49,26,11,8,1,16,1,26,5,100,97,116,97,50,244,41,93,155,173,190,
                58,30,240,223,216,8,26,205,86,4>>,
    [Result1, Result2] = deaggregate_kpl_record(R, AggRecord, AggData),

    ?assertEqual(<<"pk1">>, Result1#stream_record.partition_key),
    ?assertEqual(<<"data1">>, Result1#stream_record.data),
    ?assertEqual(666, Result1#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(0, Result1#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(0, Result1#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(2, Result1#stream_record.sequence_number#sequence_number.user_total),

    ?assertEqual(<<"pk2">>, Result2#stream_record.partition_key),
    ?assertEqual(<<"data2">>, Result2#stream_record.data),
    ?assertEqual(666, Result2#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(1, Result2#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(0, Result2#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(2, Result2#stream_record.sequence_number#sequence_number.user_total),

    ok.


deaggregate_kpl_records_v1_noagg_test() ->
    R = #{},
    Records = [
        #{<<"partitionKey">> => <<"pk1">>, <<"sequenceNumber">> => <<"666">>, <<"data">> => base64:encode(<<"data1">>)},
        #{<<"partitionKey">> => <<"pk2">>, <<"sequenceNumber">> => <<"667">>, <<"data">> => base64:encode(<<"data2">>)}
    ],
    [Result1, Result2] = deaggregate_kpl_records(R, Records),

    ?assertEqual(<<"pk1">>, Result1#stream_record.partition_key),
    ?assertEqual(<<"data1">>, Result1#stream_record.data),
    ?assertEqual(666, Result1#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(undefined, Result1#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(undefined, Result1#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(undefined, Result1#stream_record.sequence_number#sequence_number.user_total),

    ?assertEqual(<<"pk2">>, Result2#stream_record.partition_key),
    ?assertEqual(<<"data2">>, Result2#stream_record.data),
    ?assertEqual(667, Result2#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(undefined, Result2#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(undefined, Result1#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(undefined, Result2#stream_record.sequence_number#sequence_number.user_total),

    ok.


deaggregate_kpl_records_v1_agg_test() ->
    R = #{},
    Records = [
        #{<<"partitionKey">> => <<"pk1">>, <<"sequenceNumber">> => <<"666">>, <<"subSequenceNumber">> => 0, <<"data">> => base64:encode(
                <<(?KPL_AGG_MAGIC)/binary,10,3,112,107,49,10,3,112,107,50,18,4,101,104,
                  107,49,18,4,101,104,107,50,26,11,8,0,16,0,26,5,100,97,116,97,
                  49,26,11,8,1,16,1,26,5,100,97,116,97,50,244,41,93,155,173,190,
                  58,30,240,223,216,8,26,205,86,4>>)},
        #{<<"partitionKey">> => <<"pk2">>, <<"sequenceNumber">> => <<"667">>, <<"subSequenceNumber">> => 0, <<"data">> => base64:encode(
                <<(?KPL_AGG_MAGIC)/binary,10,3,112,107,51,10,3,112,107,52,18,4,101,104,
                  107,51,18,4,101,104,107,52,26,11,8,0,16,0,26,5,100,97,116,97,
                  51,26,11,8,1,16,1,26,5,100,97,116,97,52,96,124,151,102,57,163,
                  206,141,67,25,76,61,196,252,78,12>>)}
    ],
    [Result1, Result2, Result3, Result4] = deaggregate_kpl_records(R, Records),

    ?assertEqual(<<"pk1">>, Result1#stream_record.partition_key),
    ?assertEqual(<<"data1">>, Result1#stream_record.data),
    ?assertEqual(666, Result1#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(0, Result1#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(0, Result1#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(2, Result1#stream_record.sequence_number#sequence_number.user_total),

    ?assertEqual(<<"pk2">>, Result2#stream_record.partition_key),
    ?assertEqual(<<"data2">>, Result2#stream_record.data),
    ?assertEqual(666, Result2#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(1, Result2#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(0, Result2#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(2, Result2#stream_record.sequence_number#sequence_number.user_total),

    ?assertEqual(<<"pk3">>, Result3#stream_record.partition_key),
    ?assertEqual(<<"data3">>, Result3#stream_record.data),
    ?assertEqual(667, Result3#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(0, Result3#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(0, Result2#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(2, Result3#stream_record.sequence_number#sequence_number.user_total),

    ?assertEqual(<<"pk4">>, Result4#stream_record.partition_key),
    ?assertEqual(<<"data4">>, Result4#stream_record.data),
    ?assertEqual(667, Result4#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(1, Result4#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(0, Result2#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(2, Result4#stream_record.sequence_number#sequence_number.user_total),

    ok.


deaggregate_kpl_records_v2_noagg_test() ->
    R = #{},
    Records = [
        #{<<"action">> => <<"record">>,
          <<"partitionKey">> => <<"pk1">>,
          <<"sequenceNumber">> => <<"666">>,
          <<"subSequenceNumber">> => 123,
          <<"data">> => base64:encode(<<"data1">>)},
        #{<<"action">> => <<"record">>,
          <<"partitionKey">> => <<"pk2">>,
          <<"sequenceNumber">> => <<"666">>,
          <<"subSequenceNumber">> => 124,
          <<"data">> => base64:encode(<<"data2">>)}
    ],
    [Result1, Result2] = deaggregate_kpl_records(R, Records),

    ?assertEqual(<<"pk1">>, Result1#stream_record.partition_key),
    ?assertEqual(<<"data1">>, Result1#stream_record.data),
    ?assertEqual(666, Result1#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(123, Result1#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(undefined, Result2#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(undefined, Result1#stream_record.sequence_number#sequence_number.user_total),

    ?assertEqual(<<"pk2">>, Result2#stream_record.partition_key),
    ?assertEqual(<<"data2">>, Result2#stream_record.data),
    ?assertEqual(666, Result2#stream_record.sequence_number#sequence_number.base),
    ?assertEqual(124, Result2#stream_record.sequence_number#sequence_number.sub),
    ?assertEqual(undefined, Result2#stream_record.sequence_number#sequence_number.user_sub),
    ?assertEqual(undefined, Result2#stream_record.sequence_number#sequence_number.user_total),

    ok.


-endif.
