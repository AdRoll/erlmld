%%%-------------------------------------------------------------------
%%% @copyright (C) 2016, AdRoll
%%% @doc
%%%
%%%    TCP connection acceptor.  Accepts connections on Socket.  Calls AcceptCallback from
%%%    the acceptor process with successfully-accepted sockets (result ignored).  Does not
%%%    trap exits.  Exits if an accept call fails.  Sets some options before calling
%%%    AcceptCallback.
%%%
%%% @end
%%% Created : 18 Nov 2016 by Mike Watters <mike.watters@adroll.com>
%%%-------------------------------------------------------------------
-module(erlmld_tcp_acceptor).

-export([start_link/3, run/3, listen/2]).

start_link(Regname, Socket, AcceptCallback) ->
    {ok, spawn_link(?MODULE, run, [Regname, Socket, AcceptCallback])}.

run(Regname, ListenSocket, AcceptCallback) ->
    register(Regname, self()),
    loop(ListenSocket, AcceptCallback).

loop(ListenSocket, AcceptCallback) ->
    case gen_tcp:accept(ListenSocket) of
        {ok, AcceptedSocket} ->
            ok =
                inet:setopts(AcceptedSocket,
                             [{delay_send, true},
                              {send_timeout, 10000},
                              {send_timeout_close, true}]),
            AcceptCallback(AcceptedSocket),
            loop(ListenSocket, AcceptCallback);
        {error, Error} ->
            exit({accept_failed, Error})
    end.

%% given an IP specification and listen port, listen on that IP and port, returning the
%% socket and actual port we're listening on.  ip may be 'loopback' (listen on loopback
%% address) and port may be 0 (listen on a random port).
listen(ListenIP, ListenPort) ->
    {ok, Socket} =
        gen_tcp:listen(ListenPort,
                       [binary,
                        {ip, ListenIP},
                        {packet, raw},
                        {reuseaddr, true},
                        {active, false},
                        {backlog, 128}]),
    {ok, ActualPort} = inet:port(Socket),
    {ok, Socket, ActualPort}.
