-module(consume_SUITE).

-behaviour(ct_suite).

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
         end_per_testcase/2]).

-include("include/erlmld.hrl").

-behaviour(erlmld_worker).

-export([initialize/3, ready/1, process_record/2, checkpointed/3, shutdown/2]).
%% Tests
-export([simple_consume_test/1]).

all() ->
    [F
     || {F, 1} <- ?MODULE:module_info(exports),
        F /= ready,
        F /= module_info,
        F /= init_per_suite,
        F /= end_per_suite].

init_per_suite(Cfg) ->
    {ok, _} = exec:start(),
    Cfg.

end_per_suite(_Cfg) ->
    ok.

-define(SHARDS, 5).

init_per_testcase(_Case, Cfg) ->
    StreamSuffix = [rand:uniform($z - $a + 1) + $a - 1 || _ <- "string_length"],
    StreamPrefix =
        case os:getenv("STREAM_PREFIX") of
            false ->
                "erlmld_test_";
            R ->
                R
        end,
    StreamName = StreamPrefix ++ StreamSuffix,
    create_stream(StreamName, ?SHARDS),
    [{stream, StreamName} | Cfg].

end_per_testcase(_Case, Cfg) ->
    StreamName = proplists:get_value(stream, Cfg),
    timer:sleep(1000),
    delete_stream(StreamName).

simple_consume_test(Cfg) ->
    receive after 5000 ->
        ok
    end,
    StreamName = proplists:get_value(stream, Cfg),
    receive after 5000 ->
        ok
    end,
    application:set_env(erlmld, log_kcl_spam, true),
    StreamRegion =
        case os:getenv("STREAM_REGION") of
            false ->
                "us-east-1";
            R ->
                R
        end,
    Opts =
        #{app_suffix => ?FUNCTION_NAME,
          record_processor => ?MODULE,
          record_processor_data => #{test_pid => self()},
          listen_ip => any,
          listen_port => 0,
          stream_type => kinesis,
          stream_name => StreamName,
          stream_region => StreamRegion,
          initial_position => "TRIM_HORIZON",
          idle_time => 200,
          metrics_level => "DETAILED",
          metrics_dimensions => "",
          failover_time => 1000,
          ignore_unexpected_child_shards => false,
          worker_id => "",
          kcl_appname => ?MODULE,
          max_records => 10000,
          aws_credentials_provider => "DefaultAWSCredentialsProviderChain",
          max_lease_theft => 1,
          shard_sync_time => 100},
    {ok, Sup} = erlmld_sup:start_link(Opts),
    wait_init(),
    load_records(StreamName, [test, records1]),
    wait_ready(?SHARDS),
    wait_idle(2),
    exit(Sup, normal).

wait_init() ->
    receive
        initialize ->
            ok
    after 120000 ->
        ct:fail("consumer not initialized")
    end.

wait_ready(N) ->
    wait_ready(N, 120000).

wait_ready(0, _T) ->
    ok;
wait_ready(N, T) ->
    receive
        ready ->
            wait_ready(N - 1, 1000)
    after T ->
        ct:fail("~p never ready", [N])
    end.

wait_idle(MinRecs) ->
    wait_idle(MinRecs, 30000).

wait_idle(MinRecs, Timeout) when Timeout > 0 ->
    receive
        {process_record, SequenceNumber} ->
            ct:pal("process ~p", [SequenceNumber]),
            wait_idle(MinRecs - 1)
    after min(Timeout, 500) ->
        case MinRecs of
            0 ->
                ct:pal("500ms of no records, after minimum.");
            _ ->
                ct:pal("Awaiting ~p more records for ~p ms", [MinRecs, Timeout]),
                wait_idle(MinRecs, Timeout - 500)
        end
    end;
wait_idle(MinRecs, _) when MinRecs > 0 ->
    ct:fail("Still needing ~p records, no time left", [MinRecs]);
wait_idle(_, _) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%    kinesalite helpers
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_stream(StreamName, Shards) ->
    run_helper(aws,
               ["kinesis", "create-stream", "--stream-name", StreamName, "--shard-count", Shards]).

delete_stream(StreamName) ->
    run_helper(aws, ["kinesis", "delete-stream", "--stream-name", StreamName]).

load_records(StreamName, Dir) ->
    run_helper([test, bin, load_from_dir], [StreamName, get_path(Dir)]).

-define(ERRTAG(Str), "OSCMDERR" ++ Str ++ "OSCMDEND").

run_helper([_ | _] = Script, Args) ->
    run_helper_(get_path(Script), Args);
run_helper(Script, Args) ->
    run_helper([Script], Args).

run_helper_(Script, Args) ->
    Cmd = string:join([Script | fixup_path_items(Args)], " "),
    ct:pal("Running \"~s\"", [Cmd]),
    Output = os:cmd(Cmd ++ "|| echo " ++ ?ERRTAG("$?")),
    case re:run(Output, helper_re_(), [{capture, all_but_first, binary}]) of
        {match, [ErrOut, Status]} ->
            ct:fail("Failed with status ~s:~n~s", [Status, ErrOut]);
        nomatch ->
            ct:pal("Success:~n~s", [Output])
    end.

helper_re_() ->
    {ok, P} = re:compile("(.*)" ++ ?ERRTAG("(.*)"), [anchored, dotall]),
    P.

get_path(Components) ->
    filename:join(fixup_path_items(Components)).

fixup_path_items(Items) ->
    lists:map(fun fixup_path_item/1, Items).

fixup_path_item(test) ->
    code:priv_dir(erlmld) ++ "/../test";
fixup_path_item(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
fixup_path_item(Str) when is_binary(Str); is_list(Str) ->
    Str;
fixup_path_item(Int) when is_integer(Int) ->
    integer_to_list(Int).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%    erlmld_worker callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(st, {test_pid, shard, latest_seq, checkpoint_seq}).

initialize(#{test_pid := Pid}, Shard, SequenceNumber) ->
    Pid ! initialize,
    {ok,
     #st{test_pid = Pid,
         shard = Shard,
         latest_seq = SequenceNumber,
         checkpoint_seq = SequenceNumber}}.

ready(#st{test_pid = Pid} = State) ->
    Pid ! ready,
    {ok, State}.

process_record(#st{test_pid = Pid, shard = _Shard} = State,
               #stream_record{sequence_number = SequenceNumber}) ->
    Pid ! {process_record, SequenceNumber},
    {ok,
     State#st{latest_seq = SequenceNumber},
     #checkpoint{sequence_number = SequenceNumber}}.

checkpointed(#st{} = State, SequenceNumber, _Checkpoint) ->
    {ok, State#st{checkpoint_seq = SequenceNumber}}.

shutdown(_, _) ->
    ok.
