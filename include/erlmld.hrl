-ifndef(ERLMLD_HRL).
-define(ERLMLD_HRL, true).

-record(sequence_number, {
          %% overall record sequence number:
          base :: undefined | non_neg_integer() | atom(),

          %% record sub-sequence number for records using KPL aggregation:
          sub :: undefined | non_neg_integer(),

          %% record sub-sequence number for records NOT using KPL aggregation.
          %% erlmld supports its own KPL-like aggregation and will fill this
          %% one when needed. For other use cases, your code is expected to
          %% fake these when needed.
          user_sub :: undefined | non_neg_integer(),

          %% total number of records in aggregated KPL record:
          %% (user_sub will range from 0 to user_total-1)
          user_total :: undefined | non_neg_integer()
         }).

-record(checkpoint, {
          sequence_number :: undefined | sequence_number()
         }).

-record(stream_record, {
          partition_key :: binary(),
          timestamp :: non_neg_integer(),  % approximate arrival time (ms)
          delay :: non_neg_integer(),      % approximate delay between this record and tip of stream (ms)
          sequence_number :: sequence_number(),
          data :: term()
         }).

-type worker_state() :: term().
-type shard_id() :: binary().
-type sequence_number() :: #sequence_number{}.
-type stream_record() :: #stream_record{}.
-type shutdown_reason() :: terminate | zombie.
-type checkpoint() :: #checkpoint{}.

%% Types used by the flusher behavior (see erlmld_flusher.erl).
-type flusher_state() :: term().
-type flusher_token() :: term().

%% Magic number identifying aggregated KPL records.
%% (See: https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md)
%%
%% Note: we expect 0x00 instead of 0xF3 as the initial byte to prevent the KCL from
%% deaggregating the record for us, because the version of the KCL/MLD which is compatible
%% with version 1.1.1 of the dynamo streams adapter doesn't properly deaggregate (doesn't
%% include subsequence numbers in the records we see).
-define(KPL_AGG_MAGIC, <<16#00, 16#89, 16#9A, 16#C2>>).

%% magic number identifying deflate-compressed KPL record, compressed using
%% zlib:compress/1.  the KPL checksum trailer is included in the deflated data.
-define(KPL_AGG_MAGIC_DEFLATED, <<16#01, 16#89, 16#9A, 16#C2>>).

-endif.
