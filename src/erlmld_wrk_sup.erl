%%%-------------------------------------------------------------------
%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%     Worker supervisor.  Workers are gen_statem processes launched using
%%%     start_worker/2, which accepts a supervisor reference (pid or name) and an accepted
%%%     socket.
%%%
%%% @end
%%% Created : 18 Nov 2016 by Mike Watters <mike.watters@adroll.com>
%%%-------------------------------------------------------------------
-module(erlmld_wrk_sup).

-behaviour(supervisor).

%% API
-export([start_link/3, start_worker/2]).
%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Regname, RecordProcessor, RecordProcessorData) ->
    supervisor:start_link({local, Regname}, ?MODULE, [RecordProcessor, RecordProcessorData]).

start_worker(SupRef, AcceptedSocket) ->
    {ok, Pid} = Result = supervisor:start_child(SupRef, []),
    ok = erlmld_wrk_statem:accept(Pid, AcceptedSocket),
    Result.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([RecordProcessor, RecordProcessorData]) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 0, period => 1},

    Worker =
        #{id => erlmld_wrk_statem,
          start => {erlmld_wrk_statem, start_link, [RecordProcessor, RecordProcessorData]},
          restart => temporary,
          shutdown => brutal_kill},

    {ok, {SupFlags, [Worker]}}.
