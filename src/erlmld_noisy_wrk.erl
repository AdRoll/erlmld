%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%     The default example worker, which prints events and checkpoints after every 10
%%%     records.
%%%
%%% @end
%%% Created : 18 Nov 2016 by Mike Watters <mike.watters@adroll.com>

-module(erlmld_noisy_wrk).

-behaviour(erlmld_worker).

-export([initialize/3, ready/1, process_record/2, checkpointed/3, shutdown/2]).

-include("erlmld.hrl").

-record(state, {shard_id, count = 0}).

initialize(_Opaque, ShardId, ISN) ->
    State = #state{shard_id = ShardId},
    io:format("~p initialized for shard ~p at ~p~n", [self(), ShardId, ISN]),
    {ok, State}.

ready(State) ->
    {ok, State}.

process_record(#state{shard_id = ShardId, count = Count} = State,
               #stream_record{sequence_number = SN} = Record) ->
    io:format("~p (~p) got record ~p~n", [ShardId, Count, Record]),
    case Count >= 10 of
        true ->
            {ok, State#state{count = 0}, #checkpoint{sequence_number = SN}};
        false ->
            {ok, State#state{count = Count + 1}}
    end.

checkpointed(#state{shard_id = ShardId, count = Count} = State,
             SequenceNumber,
             Checkpoint) ->
    io:format("~p (~p) checkpointed at ~p (~p)~n",
              [ShardId, Count, Checkpoint, SequenceNumber]),
    {ok, State}.

shutdown(#state{shard_id = ShardId, count = Count}, Reason) ->
    io:format("~p (~p) shutting down, reason: ~p~n", [ShardId, Count, Reason]),
    case Reason of
        terminate ->
            {ok, #checkpoint{}};
        _ ->
            ok
    end.
