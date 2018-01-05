%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%     Given a path corresponding to an MLD .properties file, run a script with that path
%%%     as its argument using erlexec, assuming it should run forever.  That script should
%%%     launch an instance of the MultiLangDaemon, which interprets the properties file
%%%     and connects back to this node (one TCP connection for each shard owned by that
%%%     MLD instance).
%%%
%%% @end
%%% Created : 18 Nov 2016 by Mike Watters <mike.watters@adroll.com>

-module(erlmld_runner).

-export([start_link/3,
         run/3,
         build_properties/1]).


start_link(Regname, Pathname, StreamType) ->
    {ok, spawn_link(?MODULE, run, [Regname, Pathname, StreamType])}.


run(Regname, Pathname, StreamType) ->
    register(Regname, self()),
    {Exe, CWD} = runner_params(StreamType),
    process_flag(trap_exit, true),
    {ok, ErlPid, OsPid} = exec:run_link([Exe, Pathname],
                                        [{cd, CWD},
                                         {group, 0},
                                         kill_group,
                                         stdout, stderr]),
    error_logger:info_msg("~p launched ~p (pid ~p)~n", [Regname, Exe, OsPid]),
    {ok, SpamMP} = spam_mp(),
    loop(Regname, ErlPid, OsPid, SpamMP).


loop(Regname, ErlPid, OsPid, SpamMP) ->
    receive
        {stdout, OsPid, _Data} ->
            ok;

        {stderr, OsPid, Data} ->
            case application:get_env(erlmld, log_kcl_spam, undefined) of
                true ->
                    io:format("~p: ~s", [Regname, Data]);
                false ->
                    ok;
                undefined ->
                    ok;
                {LagerMod, LagerSink} ->
                    [LagerMod:log(LagerSink, debug, LagerMod:md(), "~p: ~s", [Regname, Line])
                     || Line <- binary:split(Data, <<"\n">>, [global]),
                        not is_spam(SpamMP, Line)],
                    ok
            end;

        {'EXIT', ErlPid, Reason} ->
            exit({child_exited, OsPid, Reason})
    end,
    loop(Regname, ErlPid, OsPid, SpamMP).


runner_params(StreamType) ->
    Runner = io_lib:format("run_~p.sh", [StreamType]),
    Pathname = priv_path(Runner),
    {Pathname, filename:dirname(Pathname)}.


priv_path(Filename) ->
    Priv = code:priv_dir(erlmld),
    lists:flatten(filename:join(Priv, Filename)).


tempdir_path(Filename) ->
    filename:join(os:getenv("TMPDIR", "/tmp"), [erlmld, $/, Filename]).


%% given a map of option values, populate the MLD properties template, creating a file
%% like "$TMPDIR/erlmld/erlmld.X.properties", where X is either "default" or the
%% app_suffix value, and return that populated pathname.
build_properties(#{app_suffix := AppSuffix} = Opts) ->
    Input = priv_path("mld.properties.in"),
    Output = tempdir_path("erlmld." ++ atom_to_list(case AppSuffix of
                                                        undefined -> default;
                                                        _ -> AppSuffix
                                                    end) ++ ".properties"),
    {ok, Template} = file:read_file(Input),
    {ok, Result} = apply_substitutions(Template, Opts),
    ok = filelib:ensure_dir(Output),
    ok = file:write_file(Output, Result),
    {ok, Output}.


%% given a binary template and a map of options, use the map to apply substitutions into
%% the template.  the map of options is used to populate shell-like variable references in
%% the template file as in the following example:
%%
%%   options:
%%
%%     #{xyzzy => <<"asdf">>, foo => undefined}
%%
%%   template contents:
%%
%%     someProperty = ${XYZZY}
%%     otherProperty = ${FOO}
%%
%%  result:
%%
%%     someProperty = asdf
%%     otherProperty =
%%
%%  Unknown variable references are errors.  To substitute an empty value, use undefined.
%%  Integers, atoms, and iodata are converted to binaries before being substituted.
%%  Other types are unsupported and ignored.
apply_substitutions(Template, Opts) ->
    Data = maps:fold(
             fun (_, V, Acc) when is_tuple(V);
                                  is_map(V) ->
                     Acc;
                 (K, V, Acc) ->
                     Var = iolist_to_binary("${" ++ string:to_upper(atom_to_list(K)) ++ "}"),
                     Val = case V of
                               undefined ->
                                   <<>>;
                               V when is_integer(V) ->
                                   integer_to_binary(V);
                               V when is_atom(V) ->
                                   atom_to_binary(V, utf8);
                               V when is_list(V) ->
                                   iolist_to_binary(V);
                               V when is_binary(V) ->
                                   V;
                               _ ->
                                   <<>>
                           end,
                     binary:replace(Acc, Var, Val, [global])
             end, Template, Opts),
    case re:run(Data, <<"\\${([^}]+)}">>, [global]) of
        {match, Groups} ->
            {error, {unknown_variables,
                     sets:to_list(
                       lists:foldl(fun ([_, {Start, Size}], Acc) ->
                                           Name = binary:part(Data, {Start, Size}),
                                           sets:add_element(Name, Acc)
                                   end, sets:new(), Groups))}};
        nomatch ->
            {ok, Data}
    end.


spam_mp() ->
    Spammy = [<<"^INFO: Received response ">>,
              <<"^INFO: Starting: Reading next message from STDIN ">>,
              <<"^INFO: Writing ProcessRecordsMessage to child process ">>,
              <<"^INFO: Message size == (40|63) bytes for shard ">>,
              <<"com.amazonaws.services.kinesis.multilang.MultiLangProtocol validateStatusMessage$">>,
              <<"com.amazonaws.services.kinesis.multilang.MessageWriter writeMessage$">>,
              <<"com.amazonaws.services.kinesis.multilang.MessageWriter call$">>,
              <<"com.amazonaws.services.kinesis.multilang.LineReaderTask call$">>],
    re:compile(lists:join(<<"|">>, Spammy)).


is_spam(_, <<>>) ->
    true;
is_spam(MP, Bin) ->
    nomatch /= re:run(Bin, MP).
