%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%     Flusher behavior.
%%%
%%%     A flusher is initialized with a ShardID and some arbitrary data:
%%%
%%%         FlusherState = my_flusher:init(ShardId, FlusherData)
%%%
%%%     The flusher maintains a batch of records. A new record can be added
%%%     using 'add_record', together with an opaque token. 'add_record' will
%%%     return an error if the current batch is full:
%%%
%%%         case my_flusher:add_record(FlusherState, StreamRecord, Token) of
%%%             {error, full} ->              %% should flush and try again
%%%             {ignored, NewFlusherState} -> %% record ignored
%%%             {ok, NewFlusherState} ->      %% record added
%%%         end
%%%
%%%     The current batch can be flushed using 'flush'. The caller gets back a
%%%     list of tokens identifying which records got flushed.  If the second
%%%     argument to flush/2 is 'full', all outstanding data must be flushed;
%%%     this is to support checkpointing on shards which processing is being
%%%     terminated.
%%%
%%%         {ok, NewFlusherState, FlushedTokens} = my_flusher:flush(FlusherState, partial)
%%%
%%%     A flusher is meant to be used as part of a 'erlmld_batch_processor'.
%%%     The batch processor handles checkpointing and decides when to trigger
%%%     flushing.
%%%
%%%     If stream volume is low, a flusher module should implement ready/1,
%%%     which if exported will be called regardless of whether any records
%%%     could be obtained from the stream.  It may return the same values as
%%%     flush/2.  If it returns a non-empty list of tokens as the third tuple
%%%     element, it is considered to have just performed a partial flush.
%%%
%%% @end
%%% Created : 20 Dec 2016 by Constantin Berzan <constantin.berzan@adroll.com>

-module(erlmld_flusher).

-include("erlmld.hrl").

-optional_callbacks([ready/1]).

-callback init(shard_id(), term()) ->
    flusher_state().

-callback add_record(flusher_state(), stream_record(), flusher_token()) ->
    {ok, flusher_state()}
        | {ignored, flusher_state()}
        | {error, full | term()}.

-callback flush(flusher_state(), partial | full) ->
    {ok, flusher_state(), list(flusher_token())}
        | {error, term()}.

-callback ready(flusher_state()) ->
    {ok, flusher_state(), list(flusher_token())}
        | {error, term()}.
