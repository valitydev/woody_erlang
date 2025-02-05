%%% @doc Internal utils
%%% @end

-module(woody_util).

-export([get_protocol_handler/2]).
-export([get_mod_opts/1]).
-export([to_binary/1]).
-export([get_rpc_type/2]).
-export([get_rpc_reply_type/1]).

-define(DEFAULT_HANDLER_OPTS, undefined).

%%

-export([span_stack_put/3]).
-export([span_stack_get/3]).
-export([span_stack_pop/2]).

%%
%% Internal API
%%
-spec get_protocol_handler(woody:role(), map()) -> module() | no_return().
get_protocol_handler(_Role, #{protocol_handler_override := Module}) when is_atom(Module) ->
    Module;
get_protocol_handler(Role, Opts) ->
    Protocol = genlib_map:get(protocol, Opts, thrift),
    Transport = genlib_map:get(transport, Opts, http),
    case {Role, Protocol, Transport} of
        {client, thrift, http} -> woody_client_thrift_v2;
        {server, thrift, http} -> woody_server_thrift_v2;
        _ -> error(badarg, [Role, Opts])
    end.

-spec get_mod_opts(woody:handler(woody:options())) -> {module(), woody:options()}.
get_mod_opts({Mod, _Opts} = Handler) when is_atom(Mod) ->
    Handler;
get_mod_opts(Mod) when is_atom(Mod) ->
    {Mod, ?DEFAULT_HANDLER_OPTS}.

-spec to_binary(atom() | list() | binary()) -> binary().
to_binary(Reason) when is_list(Reason) ->
    to_binary(Reason, <<>>);
to_binary(Reason) ->
    to_binary([Reason]).

to_binary([], Reason) ->
    Reason;
to_binary([Part | T], Reason) ->
    BinPart = genlib:to_binary(Part),
    to_binary(T, <<Reason/binary, BinPart/binary>>).

-spec get_rpc_type(woody:service(), woody:func()) -> woody:rpc_type().
get_rpc_type({Module, Service}, Function) ->
    get_rpc_reply_type(Module:function_info(Service, Function, reply_type)).

-spec get_rpc_reply_type(_ThriftReplyType) -> woody:rpc_type().
get_rpc_reply_type(oneway_void) -> cast;
get_rpc_reply_type(_) -> call.

%% OTEL context span helpers
%% NOTE Those helpers are designed specifically to manage stacking spans during
%%      woody client (or server) calls _inside_ one single process context.
%%      Thus, use of process dictionary via `otel_ctx'.

-define(OTEL_SPANS_STACK, 'spans_ctx_stack').

-type span_key() :: atom() | binary() | string().
-type maybe_span_ctx() :: opentelemetry:span_ctx() | undefined.

-spec span_stack_put(span_key(), opentelemetry:span_ctx(), otel_ctx:t()) -> otel_ctx:t().
span_stack_put(Key, SpanCtx, Context) ->
    Stack = otel_ctx:get_value(Context, ?OTEL_SPANS_STACK, []),
    Entry = {Key, SpanCtx, otel_tracer:current_span_ctx(Context)},
    otel_ctx:set_value(Context, ?OTEL_SPANS_STACK, [Entry | Stack]).

-spec span_stack_get(span_key(), otel_ctx:t(), maybe_span_ctx()) -> maybe_span_ctx().
span_stack_get(Key, Context, Default) ->
    Stack = otel_ctx:get_value(Context, ?OTEL_SPANS_STACK, []),
    case lists:keyfind(Key, 1, Stack) of
        false ->
            Default;
        {_Key, SpanCtx, _ParentSpanCtx} ->
            SpanCtx
    end.

-spec span_stack_pop(span_key(), otel_ctx:t()) ->
    {ok, opentelemetry:span_ctx(), maybe_span_ctx(), otel_ctx:t()} | {error, notfound}.
span_stack_pop(Key, Context) ->
    Stack = otel_ctx:get_value(Context, ?OTEL_SPANS_STACK, []),
    case lists:keytake(Key, 1, Stack) of
        false ->
            {error, notfound};
        {value, {_Key, SpanCtx, ParentSpanCtx}, Stack1} ->
            Context1 = otel_ctx:set_value(Context, ?OTEL_SPANS_STACK, Stack1),
            {ok, SpanCtx, ParentSpanCtx, Context1}
    end.
