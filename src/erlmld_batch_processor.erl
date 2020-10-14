%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%     Generic record processor with support for batching and checkpointing.
%%%
%%%     We use an underlying 'erlmld_flusher' module to maintain the current
%%%     batch. When the current batch is full (according to the flusher's
%%%     definition of fullness), or when 'flush_interval_ms' milliseconds have
%%%     elapsed, we ask the flusher to flush the current batch and start a new
%%%     one.
%%%
%%%     We checkpoint every 'checkpoint_interval_ms' milliseconds. After
%%%     'watchdog_timeout_ms' milliseconds with no activity (after any initial
%%%     activity), the current process is killed. Checkpointing works by
%%%     keeping track of the latest record for which itself and all
%%%     predecessors have been flushed. For example, suppose 'process_record'
%%%     has been called with the following records, and the flusher has already
%%%     flushed the ones marked with '*':
%%%
%%%         R1 R2 R3 R4 R5 R6 R7 R8 R9 R10
%%%         *  *  *     *  *     *
%%%
%%%     Then the stream will be checkpointed at (the sequence number of) R3.
%%%     For the internal mechanics, see the comment in 'note_success'.
%%%
%%% @end
%%% Created : 19 Dec 2016 by Constantin Berzan <constantin.berzan@adroll.com>
%%% based on original code by Mike Watters <mike.watters@adroll.com>
-module(erlmld_batch_processor).

-behavior(erlmld_worker).

-export([initialize/3, ready/1, process_record/2, checkpointed/3, shutdown/2]).

-include("erlmld.hrl").

-record(state,
        {%% Handler module implementing the flusher behavior.
         flusher_mod :: atom(),
         %% Flusher state (opaque to this module) passed to flusher_mod callbacks.
         flusher_state :: flusher_state(),
         %% Optional callback to call each time process_record returns a checkpoint.
         on_checkpoint :: fun((term(), shard_id()) -> term()),
         %% Optional, false by default. Tells whether to log or not every successful
         %% checkpoint from the KCL worker.
         log_checkpoints :: boolean(),
         description :: term(),
         shard_id :: shard_id(),
         count = 0, % non-ignored records seen
         checkpoints = 0, % checkpoints requested
         next_counter_checkpoint = 0, % counter value at which we can next checkpoint
         last_flush_time = os:timestamp(),
         last_checkpoint_time = os:timestamp(),
         checkpointable = gb_trees:empty(), % {Count, SequenceNo}
         flush_interval_ms,
         checkpoint_interval_ms,
         watchdog_timeout_ms,
         watchdog,
         enable_subsequence_checkpoints = false :: boolean()}).

%%%===================================================================
%%% API
%%%===================================================================

initialize(Opts, ShardId, ISN) ->
    Defaults =
        #{on_checkpoint => fun(_, _) -> ok end,
          log_checkpoints => false,
          description => undefined,
          enable_subsequence_checkpoints => false},
    #{description := Description,
      flusher_mod := FlusherMod,
      flusher_mod_data := FlusherModData,
      on_checkpoint := OnCheckpoint,
      log_checkpoints := LogCheckpoints,
      flush_interval_ms := FlushIntervalMs,
      checkpoint_interval_ms := CheckpointIntervalMs,
      watchdog_timeout_ms := WatchdogTimeoutMs,
      enable_subsequence_checkpoints := EnableSubCP} =
        maps:merge(Defaults, Opts),
    State =
        #state{flusher_mod = FlusherMod,
               flusher_state = FlusherMod:init(ShardId, FlusherModData),
               on_checkpoint = OnCheckpoint,
               log_checkpoints = LogCheckpoints,
               description = Description,
               shard_id = ShardId,
               flush_interval_ms = FlushIntervalMs,
               checkpoint_interval_ms = CheckpointIntervalMs,
               watchdog_timeout_ms = WatchdogTimeoutMs,
               enable_subsequence_checkpoints = EnableSubCP},
    error_logger:info_msg("~p initialized for shard {~p,~p} at ~p~n",
                          [self(), Description, ShardId, ISN]),
    {ok, update_watchdog(State)}.

ready(#state{flusher_mod = FMod, flusher_state = FState} = State) ->
    {ok, NFState, Tokens} = FMod:heartbeat(FState),
    NState = flusher_state(State, NFState),
    NNState =
        case Tokens of
            [] ->
                NState;
            _ ->
                note_success(note_flush(NState), Tokens)
        end,
    maybe_checkpoint(update_watchdog(NNState)).

process_record(#state{last_flush_time = LastFlush, flush_interval_ms = FlushInterval} =
                   State,
               Record) ->
    {ok, NState} =
        case {add_record(State, Record), elapsed_ms(LastFlush)} of
            {{ok, State1}, E} when E >= FlushInterval ->
                {ok, flush(State1)};
            {{ok, State1}, _} ->
                {ok, State1};
            {{error, full}, _} ->
                add_record(flush(State), Record)
        end,
    maybe_checkpoint(update_watchdog(NState)).

checkpointed(#state{log_checkpoints = LogCheckpoints} = State,
             SequenceNumber,
             Checkpoint) ->
    case LogCheckpoints of
        true ->
            error_logger:info_msg("~p checkpointed at ~p (~p)~n",
                                  [State, Checkpoint, SequenceNumber]);
        false ->
            ok
    end,
    {ok, State}.

shutdown(#state{description = Description, shard_id = ShardId, count = Count} = State,
         Reason) ->
    error_logger:info_msg("{~p,~p} (~p) shutting down, reason: ~p~n",
                          [Description, ShardId, Count, Reason]),
    case Reason of
        zombie ->
            %% we lost our lease, nothing else to do now.
            ok;
        terminate ->
            %% the shard is closing (e.g., as a result of a merge or split).  we should
            %% flush all outstanding data and checkpoint.
            NState = flush_full(State),
            case next_checkpoint(NState) of
                %% we flushed all items and are able to checkpoint.  there shouldn't be
                %% any more items left to checkpoint:
                {CPState, CPSN} ->
                    error_logger:info_msg("{~p,~p} will checkpoint at ~p~n",
                                          [Description, ShardId, CPSN]),
                    Remaining = checkpointable(CPState),
                    Remaining = gb_trees:empty(),
                    {ok, #checkpoint{sequence_number = CPSN}};
                %% nothing was checkpointable.  there shouldn't be any items left to
                %% checkpoint:
                undefined ->
                    error_logger:info_msg("{~p,~p} has nothing to checkpoint, will use latest~n",
                                          [Description, ShardId]),
                    Remaining = checkpointable(NState),
                    Remaining = gb_trees:empty(),
                    %% we checkpoint with an empty sequence number.  this will cause the
                    %% KCL to checkpoint at the most recent record it has seen on the
                    %% shard:
                    {ok, #checkpoint{}}
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

incr_count(#state{count = Count} = State) ->
    State#state{count = Count + 1}.

incr_checkpoints(#state{checkpoints = Count} = State) ->
    State#state{checkpoints = Count + 1}.

checkpointable(#state{checkpointable = CPT}) ->
    CPT.

checkpointable(State, CPT) ->
    State#state{checkpointable = CPT}.

next_counter_checkpoint(State, N) ->
    State#state{next_counter_checkpoint = N}.

flusher_state(State, FState) ->
    State#state{flusher_state = FState}.

add_record(#state{count = Count, flusher_mod = FMod, flusher_state = FState} = State,
           #stream_record{sequence_number = SN} = Record) ->
    case FMod:add_record(FState, Record, {Count, SN}) of
        {ok, NFState} ->
            {ok, incr_count(flusher_state(State, NFState))};
        {ignored, NFState} ->
            {ok, flusher_state(State, NFState)};
        {error, full} ->
            {error, full}
    end.

flush(State) ->
    flush(State, partial).

flush_full(State) ->
    flush(State, full).

flush(#state{flusher_mod = FMod, flusher_state = FState} = State, Kind) ->
    {ok, NFState, Tokens} = FMod:flush(FState, Kind),
    NState = flusher_state(State, NFState),
    note_success(note_flush(NState), Tokens).

note_success(#state{checkpointable = CPT} = State, Tokens) ->
    %% The items with the given Tokens have been successfully processed.
    %% Update the 'checkpointable' gb_tree, which we are using as a priority
    %% queue. When the time comes to checkpoint, we pop items until the next
    %% smallest item has a discontiguous id, then checkpoint at the sequence
    %% number corresponding to the most recently-popped item. This way, we can
    %% do time-based checkpointing without forcing a flush of the current
    %% pending batch.
    %%
    %% fixme; when adding a new element, also pop all smaller elements which have
    %% contiguous keys with the new element (only need to track gaps).
    NCPT =
        lists:foldl(fun({Counter, SeqNum}, Acc) -> gb_trees:insert(Counter, SeqNum, Acc) end,
                    CPT,
                    Tokens),
    checkpointable(State, NCPT).

maybe_checkpoint(#state{last_checkpoint_time = LastCheckpoint,
                        checkpoint_interval_ms = CheckpointInterval} =
                     State) ->
    case elapsed_ms(LastCheckpoint) of
        N when N >= CheckpointInterval ->
            case next_checkpoint(State) of
                {NState, SequenceNumber} ->
                    {ok, note_checkpoint(NState), #checkpoint{sequence_number = SequenceNumber}};
                undefined ->
                    {ok, State}
            end;
        _ ->
            {ok, State}
    end.

%% given a state, return undefined if nothing is ready to be checkpointed.  otherwise,
%% return {NState, SeqNo}, where SeqNo is a #sequence_number{} at which we can checkpoint
%% (i.e., all items below that SN have been successfully processed), and NState represents
%% the state after checkpointing.
next_checkpoint(State) ->
    CPT = checkpointable(State),
    case gb_trees:is_empty(CPT) of
        true ->
            undefined;
        false ->
            next_checkpoint(State, gb_trees:take_smallest(CPT), undefined)
    end.

next_checkpoint(State, {SmallestCount, _, _} = SmallestItem, LatestFinished) ->
    next_checkpoint(State, SmallestCount, SmallestItem, LatestFinished).

%% in order to checkpoint, the smallest tree element must have a key whose value is equal
%% to the `next_counter_checkpoint` field; otherwise, we can't checkpoint yet (we have an
%% initial gap).
next_checkpoint(#state{next_counter_checkpoint = N}, FirstSmallestCount, _, _)
    when FirstSmallestCount > N ->
    undefined;
%% if subsequence checkpointing is disabled, we can only checkpoint at a record which has
%% been fully processed.  while advancing through contiguous items, we keep track of the
%% highest-numbered item corresponding to a completely-processed record (which may be
%% undefined).
next_checkpoint(#state{enable_subsequence_checkpoints = EnableSubCP} = State,
                FirstSmallestCount,
                {SmallestCount, SmallestSN, CPT},
                LatestFinished) ->
    %% can checkpoint at SmallestSN if subsequence checkpointing is enabled, or if it
    %% corresponds to the last subrecord in an aggregate record, or if it corresponds to a
    %% non-aggregate record:
    CanCheckpoint = EnableSubCP orelse not is_sub_record(SmallestSN),
    %% if we can checkpoint at the current SN, use it as the latest 'finished' value:
    NLatestFinished =
        case CanCheckpoint of
            true ->
                {next_counter_checkpoint(checkpointable(State, CPT), SmallestCount + 1),
                 SmallestSN};
            false ->
                LatestFinished
        end,
    case gb_trees:is_empty(CPT) of
        true ->
            NLatestFinished;
        false ->
            NextContiguous = SmallestCount + 1,
            case gb_trees:take_smallest(CPT) of
                {NextContiguous, _, _} = Next ->
                    next_checkpoint(State, FirstSmallestCount, Next, NLatestFinished);
                _ ->
                    NLatestFinished
            end
    end.

note_checkpoint(#state{description = Description,
                       shard_id = ShardId,
                       on_checkpoint = OnCheckpoint} =
                    State) ->
    OnCheckpoint(Description, ShardId),
    incr_checkpoints(State#state{last_checkpoint_time = os:timestamp()}).

note_flush(State) ->
    State#state{last_flush_time = os:timestamp()}.

%% given an erlang timestamp, return the elapsed duration in milliseconds.
elapsed_ms(When) ->
    trunc(timer:now_diff(
              os:timestamp(), When)
              / 1.0e3).

%% start a watchdog timer, cancelling any which is outstanding.  if the timer fires, it
%% will result in the current process exiting with a reason of 'watchdog_timeout'
update_watchdog(#state{watchdog_timeout_ms = undefined} = State) ->
    State;
update_watchdog(#state{watchdog_timeout_ms = WatchdogTimeout, watchdog = Watchdog} =
                    State) ->
    case Watchdog of
        undefined ->
            ok;
        _ ->
            timer:cancel(Watchdog)
    end,
    {ok, Ref} = timer:exit_after(WatchdogTimeout, watchdog_timeout),
    State#state{watchdog = Ref}.

%% We use user_sub here instead of sub: for KPL records, those values will be
%% the same. For our own KPL-like protocol user_sub will be filled in, and
%% in other use cases it will be undefined. In both of these last cases sub
%% will be the original KCL sent (0, possible) so it can't be really used to
%% know if this is a subrecord or not.
is_sub_record(#sequence_number{user_sub = Sub, user_total = Total})
    when is_integer(Sub) andalso is_integer(Total) andalso Sub < Total - 1 ->
    true;
is_sub_record(_) ->
    false.

%%%===================================================================
%%% TESTS
%%%===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

equal_cpt(A, B) ->
    maps:from_list(
        gb_trees:to_list(checkpointable(A)))
        ==
        maps:from_list(
            gb_trees:to_list(checkpointable(B))).

checkpointing_test() ->
    State = #state{},
    ?assertEqual(undefined, next_checkpoint(State)),

    %% items 0, 1, and 3 completed and checkpointable:
    CPT0 =
        gb_trees:insert(0, 0, gb_trees:insert(1, 1, gb_trees:insert(3, 3, gb_trees:empty()))),
    Expected1 = checkpointable(State, CPT0),
    State1 = note_success(State, [{0, 0}, {1, 1}, {3, 3}]),
    ?assert(equal_cpt(Expected1, State1)),
    %% the initial checkpoint should only cover items 0-1 due to the gap (2 not
    %% checkpointable yet), so item 3 should still be 'checkpointable':
    CPT1 = gb_trees:delete(0, gb_trees:delete(1, CPT0)),

    AfterCheckpoint1 = next_counter_checkpoint(checkpointable(State, CPT1), 2),
    ?assertEqual({AfterCheckpoint1, 1}, next_checkpoint(State1)),

    %% after completing item 2, the next checkpoint should be at sequence number 3,
    %% because the gap between successfully-processed items has been closed:
    CPT2 = gb_trees:insert(2, 2, CPT1),
    Expected2 = checkpointable(AfterCheckpoint1, CPT2),
    State2 = note_success(AfterCheckpoint1, [{2, 2}]),
    ?assert(equal_cpt(Expected2, State2)),
    AfterCheckpoint2 =
        next_counter_checkpoint(checkpointable(AfterCheckpoint1, gb_trees:empty()), 4),
    ?assertEqual({AfterCheckpoint2, 3}, next_checkpoint(State2)).

checkpointing_subrecord_test() ->
    State = #state{enable_subsequence_checkpoints = false},
    ?assertEqual(undefined, next_checkpoint(State)),

    SN0 = #sequence_number{user_sub = 0, user_total = 3},
    SN1 = #sequence_number{user_sub = 1, user_total = 3},
    SN2 = #sequence_number{user_sub = 2, user_total = 3},
    SN3 = #sequence_number{},

    %% items 0 and 2 completed and checkpointable, but not all subrecords have completed,
    %% so we can't checkpoint yet:
    State1 = note_success(State, [{0, SN0}, {2, SN2}]),
    ?assertEqual(undefined, next_checkpoint(State1)),

    %% item 1 is the last pending subrecord in an aggregate record, and so item 2 should
    %% be the next checkpoint:
    State2 = note_success(State1, [{1, SN1}]),
    {State3, SN2} = next_checkpoint(State2),

    %% item 3 is not a subrecord, so is checkpointable:
    State4 = note_success(State3, [{3, SN3}]),
    {_State5, SN3} = next_checkpoint(State4),

    %% if subsequence checkpointing is enabled, we can checkpoint at subrecords:
    State6 = State#state{enable_subsequence_checkpoints = true},
    State7 = note_success(State6, [{0, SN0}, {2, SN2}]),
    {State8, SN0} = next_checkpoint(State7),

    State9 = note_success(State8, [{1, SN1}]),
    {_State10, SN2} = next_checkpoint(State9).

watchdog_test() ->
    process_flag(trap_exit, true),
    State = update_watchdog(#state{watchdog_timeout_ms = 200}),
    receive
        {'EXIT', _, watchdog_timeout} ->
            error("unexpected watchdog trigger")
        after 100 ->
            ok
    end,
    update_watchdog(State),
    receive
        {'EXIT', _, watchdog_timeout} ->
            ok
        after 400 ->
            error("watchdog failed to trigger")
    end.

is_sub_record_test() ->
    ?assertEqual(true, is_sub_record(#sequence_number{user_sub = 0, user_total = 2})),
    ?assertEqual(false, is_sub_record(#sequence_number{user_sub = 1, user_total = 2})),
    ?assertEqual(false,
                 is_sub_record(#sequence_number{user_sub = undefined, user_total = undefined})).

-endif.
