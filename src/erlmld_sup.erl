-module(erlmld_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(WORKER_SUP, erlmld_wrk_sup).
-define(ACCEPTOR, erlmld_tcp_acceptor).
-define(RUNNER, erlmld_runner).

%%====================================================================
%% API functions
%%====================================================================

start_link(Opts) ->
    Regname = regname(?MODULE, Opts),
    supervisor:start_link({local, Regname}, ?MODULE, [Regname, Opts]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([Regname, #{record_processor := RecordProcessor,
                 record_processor_data := RecordProcessorData,
                 listen_ip := ListenIP,
                 listen_port := ListenPort,
                 stream_type := StreamType} = Opts]) ->
    WorkerSupName = regname(?WORKER_SUP, Opts),
    AcceptorName = regname(?ACCEPTOR, Opts),
    RunnerName = regname(?RUNNER, Opts),

    {ok, ListenSocket, ActualPort} = ?ACCEPTOR:listen(ListenIP, ListenPort),
    error_logger:info_msg("~p listening on ~p~n", [Regname, ActualPort]),

    %% prepare MLD .properties file:
    {ok, PropertiesPathname} = ?RUNNER:build_properties(maps:put(port, ActualPort, Opts)),

    SupFlags = #{strategy => rest_for_one,
                 intensity => 10,
                 period => 10},

    StartWorker = fun (AcceptedSocket) ->
                          ?WORKER_SUP:start_worker(WorkerSupName, AcceptedSocket)
                  end,

    WorkerSup = #{id => wrk_sup,
                  type => supervisor,
                  shutdown => infinity,
                  start => {?WORKER_SUP, start_link, [WorkerSupName, RecordProcessor, RecordProcessorData]}},

    TcpAcceptor = #{id => tcp_acceptor,
                    type => worker,
                    shutdown => brutal_kill,
                    start => {?ACCEPTOR, start_link, [AcceptorName, ListenSocket, StartWorker]}},

    MLDRunner = #{id => mld_runner,
                  type => worker,
                  shutdown => brutal_kill,
                  start => {?RUNNER, start_link, [RunnerName, PropertiesPathname, StreamType]}},

    ChildSpecs = [WorkerSup, TcpAcceptor, MLDRunner],

    {ok, {SupFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

regname(Prefix, #{app_suffix := Suffix}) when Suffix /= undefined ->
    binary_to_atom(<<(atom_to_binary(Prefix, utf8))/binary,
                     "_",
                     (atom_to_binary(Suffix, utf8))/binary>>, utf8);
regname(Prefix, _) ->
    Prefix.
