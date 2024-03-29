-module(erlmld_app).

-behaviour(application).

-export([start/2, stop/1, ensure_all_deps_started/0]).

%% application callback.  load default configuration from application environment, and
%% start erlmd_sup with that configuration.
start(_StartType, []) ->
    Opts =
        maps:from_list(
            application:get_all_env(erlmld)),
    erlmld_sup:start_link(Opts).

stop(_State) ->
    ok.

ensure_all_deps_started() ->
    {ok, Deps} = application:get_key(erlmld, applications),
    lists:foreach(fun(Dep) -> {ok, _} = application:ensure_all_started(Dep) end, Deps),
    ok.
