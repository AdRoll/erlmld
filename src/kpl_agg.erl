%%%-------------------------------------------------------------------
%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%     Kinesis record aggregator.
%%%
%%%     Follows the KPL aggregated record format:
%%%     https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
%%%
%%%     This is an Erlang port of the aggregation functionality from:
%%%     https://pypi.python.org/pypi/aws_kinesis_agg/1.0.0
%%%
%%%     Creating a new aggregator:
%%%
%%%         Agg = kpl_agg:new()
%%%
%%%     Adding user records to an aggregator (the aggregator will emit an
%%%     aggregated record when it is full):
%%%
%%%         case kpl_agg:add(Agg, Record) of
%%%             {undefined, NewAgg} -> ...
%%%             {FullAggRecord, NewAgg} -> ...
%%%         end
%%%
%%%     You can also use kpl:add_all to add multiple records at once. A
%%%     <pre>Record</pre> is a {PartitionKey, Data} tuple or a {PartitionKey, Data,
%%%     ExplicitHashKey} tuple.
%%%
%%%     Getting the current aggregated record (e.g. to get the last aggregated
%%%     record when you have no more user records to add):
%%%
%%%         case kpl_agg:finish(Agg) of
%%%             {undefined, Agg} -> ...
%%%             {AggRecord, NewAgg} -> ...
%%%         end
%%%
%%%     The result currently uses a non-standard magic prefix to prevent the KCL from
%%%     deaggregating the record automatically.  To use compression, instantiate the
%%%     aggregator using kpl_agg:new(true), which uses another
%%%     non-standard magic prefix.
%%%
%%% @end
%%% Created: 12 Dec 2016 by Constantin Berzan <constantin.berzan@adroll.com>
%%%-------------------------------------------------------------------
-module(kpl_agg).

%% API
-export([new/0, new/1, count/1, size_bytes/1, finish/1, add/2, add_all/2]).

-define(MD5_DIGEST_BYTES, 16).
%% From http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html:
-define(KINESIS_MAX_BYTES_PER_RECORD, 1 bsl 20).

-include("erlmld.hrl").
-include("kpl_agg_pb.hrl").

%% A set of keys, mapping each key to a unique index.
-record(keyset,
        {rev_keys = [] :: [binary()],    %% list of known keys in reverse order
         rev_keys_length = 0 :: non_neg_integer(), %% length of the rev_keys list
         key_to_index = maps:new() :: map()}). %% maps each known key to a 0-based index
%% Internal state of a record aggregator. It stores an aggregated record that
%% is "in progress", i.e. it is possible to add more user records to it.
-record(state,
        {num_user_records = 0 :: non_neg_integer(),
         agg_size_bytes = 0 :: non_neg_integer(),
         %% The aggregated record's PartitionKey and ExplicitHashKey are the
         %% PartitionKey and ExplicitHashKey of the first user record added.
         agg_partition_key = undefined :: undefined | binary(),
         agg_explicit_hash_key = undefined :: undefined | binary(),
         %% Keys seen in the user records added so far.
         partition_keyset = #keyset{} :: #keyset{},
         explicit_hash_keyset = #keyset{} :: #keyset{},
         %% List if user records added so far, in reverse order.
         rev_records = [] :: [#'Record'{}],
         should_deflate = false}).

%%%===================================================================
%%% API
%%%===================================================================

new() ->
    new(false).

new(ShouldDeflate) ->
    #state{should_deflate = ShouldDeflate}.

count(#state{num_user_records = Num} = _State) ->
    Num.

size_bytes(#state{agg_size_bytes = Size, agg_partition_key = PK} = _State) ->
    PKSize =
        case PK of
            undefined ->
                0;
            _ ->
                byte_size(PK)
        end,
    byte_size(?KPL_AGG_MAGIC)
    + Size
    + ?MD5_DIGEST_BYTES
    + PKSize
    + byte_size(kpl_agg_pb:encode_msg(#'AggregatedRecord'{})).

finish(#state{num_user_records = 0} = State) ->
    {undefined, State};
finish(#state{agg_partition_key = AggPK,
              agg_explicit_hash_key = AggEHK,
              should_deflate = ShouldDeflate} =
           State) ->
    AggRecord = {AggPK, serialize_data(State, ShouldDeflate), AggEHK},
    {AggRecord, new(ShouldDeflate)}.

add(State, {PartitionKey, Data} = _Record) ->
    add(State, {PartitionKey, Data, undefined});
add(State, {PartitionKey, Data, ExplicitHashKey} = _Record) ->
    case {calc_record_size(State, PartitionKey, Data, ExplicitHashKey), size_bytes(State)} of
        {RecSize, _} when RecSize > ?KINESIS_MAX_BYTES_PER_RECORD ->
            error("input record too large to fit in a single Kinesis record");
        {RecSize, CurSize} when RecSize + CurSize > ?KINESIS_MAX_BYTES_PER_RECORD ->
            {FullRecord, State1} = finish(State),
            State2 = add_record(State1, PartitionKey, Data, ExplicitHashKey, RecSize),
            {FullRecord, State2};
        {RecSize, _} ->
            State1 = add_record(State, PartitionKey, Data, ExplicitHashKey, RecSize),
            %% fixme; make size calculations more accurate
            case size_bytes(State1) > ?KINESIS_MAX_BYTES_PER_RECORD - 64 of
                true ->
                    %% size estimate is almost the limit, finish & retry:
                    {FullRecord, State2} = finish(State),
                    State3 = add_record(State2, PartitionKey, Data, ExplicitHashKey, RecSize),
                    {FullRecord, State3};
                false ->
                    {undefined, State1}
            end
    end.

add_all(State, Records) ->
    {RevAggRecords, NState} =
        lists:foldl(fun(Record, {RevAggRecords, Agg}) ->
                       case add(Agg, Record) of
                           {undefined, NewAgg} ->
                               {RevAggRecords, NewAgg};
                           {AggRecord, NewAgg} ->
                               {[AggRecord | RevAggRecords], NewAgg}
                       end
                    end,
                    {[], State},
                    Records),
    {lists:reverse(RevAggRecords), NState}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Calculate how many extra bytes the given user record would take, when added
%% to the current aggregated record. This calculation has to know about KPL and
%% Protobuf internals.
calc_record_size(#state{partition_keyset = PartitionKeySet,
                        explicit_hash_keyset = ExplicitHashKeySet} =
                     _State,
                 PartitionKey,
                 Data,
                 ExplicitHashKey) ->
    %% How much space we need for the PK:
    PKLength = byte_size(PartitionKey),
    PKSize =
        case is_key(PartitionKey, PartitionKeySet) of
            true ->
                0;
            false ->
                1 + varint_size(PKLength) + PKLength
        end,

    %% How much space we need for the EHK:
    EHKSize =
        case ExplicitHashKey of
            undefined ->
                0;
            _ ->
                EHKLength = byte_size(ExplicitHashKey),
                case is_key(ExplicitHashKey, ExplicitHashKeySet) of
                    true ->
                        0;
                    false ->
                        1 + varint_size(EHKLength) + EHKLength
                end
        end,

    %% How much space we need for the inner record:
    PKIndexSize = 1 + varint_size(potential_index(PartitionKey, PartitionKeySet)),
    EHKIndexSize =
        case ExplicitHashKey of
            undefined ->
                0;
            _ ->
                1 + varint_size(potential_index(ExplicitHashKey, ExplicitHashKeySet))
        end,
    DataLength = byte_size(Data),
    DataSize = 1 + varint_size(DataLength) + DataLength,
    InnerSize = PKIndexSize + EHKIndexSize + DataSize,

    %% How much space we need for the entire record:
    PKSize + EHKSize + 1 + varint_size(InnerSize) + InnerSize.

%% Calculate how many bytes are needed to represent the given integer in a
%% Protobuf message.
varint_size(Integer) when Integer >= 0 ->
    NumBits = max(num_bits(Integer, 0), 1),
    (NumBits + 6) div 7.

%% Recursively compute the number of bits needed to represent an integer.
num_bits(0, Acc) ->
    Acc;
num_bits(Integer, Acc) when Integer >= 0 ->
    num_bits(Integer bsr 1, Acc + 1).

%% Helper for add; do not use directly.
add_record(#state{partition_keyset = PKSet,
                  explicit_hash_keyset = EHKSet,
                  rev_records = RevRecords,
                  num_user_records = NumUserRecords,
                  agg_size_bytes = AggSize,
                  agg_partition_key = AggPK,
                  agg_explicit_hash_key = AggEHK} =
               State,
           PartitionKey,
           Data,
           ExplicitHashKey,
           NewRecordSize) ->
    {PKIndex, NewPKSet} = get_or_add_key(PartitionKey, PKSet),
    {EHKIndex, NewEHKSet} = get_or_add_key(ExplicitHashKey, EHKSet),
    NewRecord =
        #'Record'{partition_key_index = PKIndex,
                  explicit_hash_key_index = EHKIndex,
                  data = Data},
    State#state{partition_keyset = NewPKSet,
                explicit_hash_keyset = NewEHKSet,
                rev_records = [NewRecord | RevRecords],
                num_user_records = 1 + NumUserRecords,
                agg_size_bytes = NewRecordSize + AggSize,
                agg_partition_key = first_defined(AggPK, PartitionKey),
                agg_explicit_hash_key = first_defined(AggEHK, ExplicitHashKey)}.

first_defined(undefined, Second) ->
    Second;
first_defined(First, _) ->
    First.

serialize_data(#state{partition_keyset = PKSet,
                      explicit_hash_keyset = EHKSet,
                      rev_records = RevRecords} =
                   _State,
               ShouldDeflate) ->
    ProtobufMessage =
        #'AggregatedRecord'{partition_key_table = key_list(PKSet),
                            explicit_hash_key_table = key_list(EHKSet),
                            records = lists:reverse(RevRecords)},
    SerializedData = kpl_agg_pb:encode_msg(ProtobufMessage),
    Checksum = crypto:hash(md5, SerializedData),
    case ShouldDeflate of
        true ->
            <<?KPL_AGG_MAGIC_DEFLATED/binary,
              (zlib:compress(<<SerializedData/binary, Checksum/binary>>))/binary>>;
        false ->
            <<?KPL_AGG_MAGIC/binary, SerializedData/binary, Checksum/binary>>
    end.

%%%===================================================================
%%% Internal functions for keysets
%%%===================================================================

is_key(Key, #keyset{key_to_index = KeyToIndex} = _KeySet) ->
    maps:is_key(Key, KeyToIndex).

get_or_add_key(undefined, KeySet) ->
    {undefined, KeySet};
get_or_add_key(Key,
               #keyset{rev_keys = RevKeys,
                       rev_keys_length = Length,
                       key_to_index = KeyToIndex} =
                   KeySet) ->
    case maps:get(Key, KeyToIndex, not_found) of
        not_found ->
            NewKeySet =
                KeySet#keyset{rev_keys = [Key | RevKeys],
                              rev_keys_length = Length + 1,
                              key_to_index = maps:put(Key, Length, KeyToIndex)},
            {Length, NewKeySet};
        Index ->
            {Index, KeySet}
    end.

potential_index(Key,
                #keyset{rev_keys_length = Length, key_to_index = KeyToIndex} = _KeySet) ->
    case maps:get(Key, KeyToIndex, not_found) of
        not_found ->
            Length;
        Index ->
            Index
    end.

key_list(#keyset{rev_keys = RevKeys} = _KeySet) ->
    lists:reverse(RevKeys).

%%%===================================================================
%%% TESTS
%%%===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

varint_size_test() ->
    %% Reference values obtained using
    %% aws_kinesis_agg.aggregator._calculate_varint_size().
    ?assertEqual(1, varint_size(0)),
    ?assertEqual(1, varint_size(1)),
    ?assertEqual(1, varint_size(127)),
    ?assertEqual(2, varint_size(128)),
    ?assertEqual(4, varint_size(9999999)),
    ?assertEqual(6, varint_size(999999999999)),
    ok.

keyset_test() ->
    KeySet0 = #keyset{},
    ?assertEqual([], key_list(KeySet0)),
    ?assertEqual(false, is_key(<<"foo">>, KeySet0)),
    ?assertEqual(0, potential_index(<<"foo">>, KeySet0)),

    {0, KeySet1} = get_or_add_key(<<"foo">>, KeySet0),
    ?assertEqual([<<"foo">>], key_list(KeySet1)),
    ?assertEqual(true, is_key(<<"foo">>, KeySet1)),
    {0, KeySet1} = get_or_add_key(<<"foo">>, KeySet1),
    ?assertEqual(1, potential_index(<<"bar">>, KeySet1)),

    {1, KeySet2} = get_or_add_key(<<"bar">>, KeySet1),
    ?assertEqual([<<"foo">>, <<"bar">>], key_list(KeySet2)),
    ?assertEqual(true, is_key(<<"foo">>, KeySet2)),
    ?assertEqual(true, is_key(<<"bar">>, KeySet2)),
    {0, KeySet2} = get_or_add_key(<<"foo">>, KeySet2),
    {1, KeySet2} = get_or_add_key(<<"bar">>, KeySet2),
    ?assertEqual(2, potential_index(<<"boom">>, KeySet2)),

    ok.

empty_aggregator_test() ->
    Agg = new(),
    ?assertEqual(0, count(Agg)),
    ?assertEqual(4 + 16, size_bytes(Agg)),  % magic and md5
    {undefined, Agg} = finish(Agg),
    ok.

simple_aggregation_test() ->
    Agg0 = new(),
    {undefined, Agg1} = add(Agg0, {<<"pk1">>, <<"data1">>, <<"ehk1">>}),
    {undefined, Agg2} = add(Agg1, {<<"pk2">>, <<"data2">>, <<"ehk2">>}),
    {AggRecord, Agg3} = finish(Agg2),
    ?assertEqual(0, count(Agg3)),
    %% Reference values obtained using priv/kpl_agg_tests_helper.py.
    RefPK = <<"pk1">>,
    RefEHK = <<"ehk1">>,
    RefData =
        <<?KPL_AGG_MAGIC/binary, 10, 3, 112, 107, 49, 10, 3, 112, 107, 50, 18, 4, 101, 104, 107,
          49, 18, 4, 101, 104, 107, 50, 26, 11, 8, 0, 16, 0, 26, 5, 100, 97, 116, 97, 49, 26, 11, 8,
          1, 16, 1, 26, 5, 100, 97, 116, 97, 50, 244, 41, 93, 155, 173, 190, 58, 30, 240, 223, 216,
          8, 26, 205, 86, 4>>,
    ?assertEqual({RefPK, RefData, RefEHK}, AggRecord),
    ok.

aggregate_many(Records) ->
    {AggRecords, Agg} = add_all(new(), Records),
    case finish(Agg) of
        {undefined, _} ->
            AggRecords;
        {LastAggRecord, _} ->
            AggRecords ++ [LastAggRecord]
    end.

shared_keys_test() ->
    [AggRecord] =
        aggregate_many([{<<"alpha">>, <<"data1">>, <<"zulu">>},
                        {<<"beta">>, <<"data2">>, <<"yankee">>},
                        {<<"alpha">>, <<"data3">>, <<"xray">>},
                        {<<"charlie">>, <<"data4">>, <<"yankee">>},
                        {<<"beta">>, <<"data5">>, <<"zulu">>}]),
    %% Reference values obtained using priv/kpl_agg_tests_helper.py.
    RefPK = <<"alpha">>,
    RefEHK = <<"zulu">>,
    RefData =
        <<?KPL_AGG_MAGIC/binary, 10, 5, 97, 108, 112, 104, 97, 10, 4, 98, 101, 116, 97, 10, 7, 99,
          104, 97, 114, 108, 105, 101, 18, 4, 122, 117, 108, 117, 18, 6, 121, 97, 110, 107, 101,
          101, 18, 4, 120, 114, 97, 121, 26, 11, 8, 0, 16, 0, 26, 5, 100, 97, 116, 97, 49, 26, 11,
          8, 1, 16, 1, 26, 5, 100, 97, 116, 97, 50, 26, 11, 8, 0, 16, 2, 26, 5, 100, 97, 116, 97,
          51, 26, 11, 8, 2, 16, 1, 26, 5, 100, 97, 116, 97, 52, 26, 11, 8, 1, 16, 0, 26, 5, 100, 97,
          116, 97, 53, 78, 67, 160, 206, 22, 1, 33, 154, 3, 6, 110, 235, 9, 229, 53, 100>>,
    ?assertEqual({RefPK, RefData, RefEHK}, AggRecord),
    ok.

record_fullness_test() ->
    Data1 = list_to_binary(["X" || _ <- lists:seq(1, 500000)]),
    Data2 = list_to_binary(["Y" || _ <- lists:seq(1, 600000)]),
    Data3 = list_to_binary(["Z" || _ <- lists:seq(1, 200000)]),

    Agg0 = new(),
    {undefined, Agg1} = add(Agg0, {<<"pk1">>, Data1, <<"ehk1">>}),
    {{AggPK1, _AggData1, AggEHK1}, Agg2} = add(Agg1, {<<"pk2">>, Data2, <<"ehk2">>}),
    {undefined, Agg3} = add(Agg2, {<<"pk3">>, Data3, <<"ehk3">>}),
    {{AggPK2, _AggData2, AggEHK2}, _} = finish(Agg3),

    %% Reference values obtained using priv/kpl_agg_tests_helper.py.
    % fixme; these comparisons will fail as long as we're using the wrong kpl magic.
    %RefChecksum1 = <<198,6,88,216,8,244,159,59,223,14,247,208,138,137,64,118>>,
    %RefChecksum2 = <<89,148,130,126,150,23,148,18,38,230,176,182,93,186,150,69>>,
    ?assertEqual(<<"pk1">>, AggPK1),
    ?assertEqual(<<"ehk1">>, AggEHK1),
    %?assertEqual(RefChecksum1, crypto:hash(md5, AggData1)),
    ?assertEqual(<<"pk2">>, AggPK2),
    ?assertEqual(<<"ehk2">>, AggEHK2),
    %?assertEqual(RefChecksum2, crypto:hash(md5, AggData2)),
    ok.

full_record_test() ->
    Fill =
        fun F(Acc) ->
                PK = integer_to_binary(rand:uniform(1000)),
                Data =
                    << <<(integer_to_binary(rand:uniform(128)))/binary>>
                       || _ <- lists:seq(1, 1 + rand:uniform(1000)) >>,
                case add(Acc, {PK, Data}) of
                    {undefined, NAcc} ->
                        F(NAcc);
                    {Full, _} ->
                        Full
                end
        end,
    {PK, Data, _} = Fill(new()),
    Total = byte_size(PK) + byte_size(Data),
    ?assert(Total =< ?KINESIS_MAX_BYTES_PER_RECORD),
    ?assert(Total >= ?KINESIS_MAX_BYTES_PER_RECORD - 2048).

deflate_test() ->
    Agg0 = new(true),
    {undefined, Agg1} = add(Agg0, {<<"pk1">>, <<"data1">>, <<"ehk1">>}),
    {{_, Data, _}, _} = finish(Agg1),
    <<Magic:4/binary, Deflated/binary>> = Data,
    ?assertEqual(?KPL_AGG_MAGIC_DEFLATED, Magic),
    Inflated = zlib:uncompress(Deflated),
    ProtoMsg = binary:part(Inflated, 0, size(Inflated) - 16),
    Checksum = binary:part(Inflated, size(Inflated), -16),
    ?assertEqual(Checksum, crypto:hash(md5, ProtoMsg)),
    #'AggregatedRecord'{records = [R1]} = kpl_agg_pb:decode_msg(ProtoMsg, 'AggregatedRecord'),
    #'Record'{data = RecordData} = R1,
    ?assertEqual(<<"data1">>, RecordData).

-endif.
