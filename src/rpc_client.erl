%%% @doc Client API
%%% @end

-module(rpc_client).

-behaviour(supervisor).

%% API
-export([new/2]).
-export([make_child_client/2]).
-export([next/1]).

-export([call/3]).
-export([call_safe/3]).
-export([call_async/5]).

-export_type([client/0, options/0, result_ok/0, result_error/0]).

-define(ROOT_REQ_PARENT_ID, <<"undefined">>).

%% Internal API
-export([init_call_async/4, do_call_async/4]).

%% supervisor callbacks
-export([init/1]).

%% behaviour definition
-callback call(client(), request(), options()) -> result_ok() | no_return().


%%
%% API
%%
-type client() :: #{  %% all elements are mandatory
    root_rpc      => boolean(),
    span_id       => rpc_t:req_id() | undefined,
    trace_id      => rpc_t:req_id(),
    parent_id     => rpc_t:req_id(),
    event_handler => rpc_t:handler(),
    seq           => non_neg_integer()
}.
-type stacktrace() :: list().

-type result_ok() :: {ok, _Response, client()} | {ok, client()}.

-type result_error() ::
    {throw, rpc_thrift_client:thrift_except(), client()} |
    {error, rpc_thrift_http_transport:error(), client()} |
    {error, _Error, client(), stacktrace()}.

-type request() :: any().

-type options() :: #{
    protocol  => thrift, %% optional
    transport => http, %% optional
    url       => rpc_t:url() %% mandatory
}.

-type callback() :: fun((result_ok() | result_error()) -> _).

-spec new(rpc_t:req_id(), rpc_t:handler()) -> client().
new(ReqId, EventHandler) ->
    #{
        root_rpc      => true,
        span_id       => ReqId,
        trace_id      => ReqId,
        parent_id     => ?ROOT_REQ_PARENT_ID,
        seq           => 0,
        event_handler => EventHandler
    }.

-spec make_child_client(rpc_t:rpc_id(), rpc_t:handler()) -> client().
make_child_client(#{span_id := ReqId, trace_id := TraceId}, EventHandler) ->
    #{
        root_rpc      => false,
        span_id       => undefined,
        trace_id      => TraceId,
        parent_id     => ReqId,
        seq           => 0,
        event_handler => EventHandler
    }.

-spec next(client()) -> client().
next(Client = #{root_rpc := true}) ->
    Client;
next(Client = #{root_rpc := false, seq := Seq}) ->
    NextSeq = Seq +1,
    Client#{span_id => make_req_id(NextSeq), seq => NextSeq}.

-spec call(client(), request(), options()) -> result_ok() |
    no_return().  %% throw:{rpc_thrift_client:thrift_except() , client()} |
                  %% error:{rpc_thrift_client:call_error()    , client()} |
                  %% error:{rpc_thrift_client:badarg_error()  , client()}.
call(Client, Request, Options) ->
    ProtocolHandler = rpc_t:get_protocol_handler(client, Options),
    ProtocolHandler:call(next(Client), Request, Options).

-spec call_safe(client(), request(), options()) -> result_ok() | result_error().
call_safe(Client, Request, Options) ->
    try call(Client, Request, Options)
    catch
        %% valid thrift exception
        throw:{Except, Client1} ->
            {throw, Except, Client1};
        %% rpc send failed
        error:{TError = {transport_error, _}, Client1} ->
            {error, TError, Client1};
        %% thrift protocol error
        error:{PError = {protocol_error, _}, Client1} ->
            {error, PError, Client1, erlang:get_stacktrace()};
        %% what else could have happened?
        Class:{Reason, Client1} ->
            {Class, Reason, Client1, erlang:get_stacktrace()}
    end.

-spec call_async(rpc_t:sup_ref(), callback(), client(), request(), options()) ->
    {ok, pid(), client()} | {error, _}.
call_async(Sup, Callback, Client, Request, Options) ->
    _ = rpc_t:get_protocol_handler(client, Options),
    SupervisorSpec = #{
        id       => {?MODULE, rpc_clients_sup},
        start    => {supervisor, start_link, [?MODULE, rpc_client_sup]},
        restart  => permanent,
        shutdown => infinity,
        type     => supervisor,
        modules  => [?MODULE]
    },
    ClientSup = case supervisor:start_child(Sup, SupervisorSpec) of
        {ok, Pid} -> Pid;
        {error, {already_started, Pid}} -> Pid
    end,
    supervisor:start_child(ClientSup,
        [Callback, Client, Request, Options]).

%%
%% Internal API
%%
-spec init_call_async(callback(), client(), request(), options()) -> {ok, pid(), client()}.
init_call_async(Callback, Client, Request, Options) ->
    proc_lib:start_link(?MODULE, do_call_async, [Callback, Client, Request, Options]).

-spec do_call_async(callback(), client(), request(), options()) -> _.
do_call_async(Callback, Client, Request, Options) ->
    proc_lib:init_ack({ok, self(), next(Client)}),
    Callback(call_safe(Client, Request, Options)).

%%
%% Supervisor callbacks
%%
-spec init(rpc_client_sup) -> {ok, {#{}, [#{}, ...]}}.
init(rpc_client_sup) ->
    {ok, {
        #{
            strategy  => simple_one_for_one,
            intensity => 1,
            period    => 1
        },
        [#{
            id       => undefined,
            start    => {?MODULE, init_call_async, []},
            restart  => temporary,
            shutdown => brutal_kill,
            type     => worker,
            modules  => []
        }]
    }
}.


%%
%% Internal functions
%%
-spec make_req_id(non_neg_integer()) -> rpc_t:req_id().
make_req_id(Seq) ->
    BinSeq = genlib:to_binary(Seq),
    SnowFlake = snowflake:serialize(snowflake:new(?MODULE)),
    <<BinSeq/binary, $:, SnowFlake/binary>>.
