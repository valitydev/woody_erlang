-module(woody_event_handler_otel).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").
-include("woody_defs.hrl").

-behaviour(woody_event_handler).

-export([handle_event/4]).

-spec handle_event(Event, RpcID, Meta, Opts) -> ok when
    Event :: woody_event_handler:event(),
    RpcID :: woody:rpc_id() | undefined,
    Meta :: woody_event_handler:event_meta(),
    Opts :: woody:options().

-define(IS_SPAN_START(Event), Event =:= ?EV_CALL_SERVICE orelse Event =:= ?EV_INVOKE_SERVICE_HANDLER).
-define(IS_SPAN_END(Event), Event =:= ?EV_SERVICE_RESULT orelse Event =:= ?EV_SERVICE_HANDLER_RESULT).
-define(IS_CLIENT_INTERNAL(Event),
    Event =:= ?EV_CLIENT_CACHE_HIT orelse
        Event =:= ?EV_CLIENT_CACHE_MISS orelse
        Event =:= ?EV_CLIENT_CACHE_UPDATE orelse
        Event =:= ?EV_CLIENT_SEND orelse
        Event =:= ?EV_CLIENT_RECEIVE
).

%% Client events
handle_event(Event, RpcID, #{url := Url} = _Meta, _Opts) when ?IS_CLIENT_INTERNAL(Event) ->
    with_span(otel_ctx:get_current(), mk_ref(RpcID), fun(SpanCtx) ->
        _ = otel_span:add_event(SpanCtx, atom_to_binary(Event), #{url => Url})
    end);
%% Internal error handling
handle_event(?EV_INTERNAL_ERROR, RpcID, #{error := Error, class := Class, reason := Reason} = Meta, _Opts) ->
    Stacktrace = maps:get(stack, Meta, []),
    Details = io_lib:format("~ts: ~ts", [Error, Reason]),
    with_span(otel_ctx:get_current(), mk_ref(RpcID), fun(SpanCtx) ->
        _ = otel_span:record_exception(SpanCtx, genlib:define(Class, error), Details, Stacktrace, #{}),
        otel_maybe_cleanup(Meta, SpanCtx)
    end);
%% Registers span starts/ends for woody client calls and woody server function invocations.
handle_event(Event, RpcID, Meta, _Opts) when ?IS_SPAN_START(Event) ->
    Tracer = opentelemetry:get_application_tracer(?MODULE),
    span_start(Tracer, otel_ctx:get_current(), mk_ref(RpcID), mk_name(Meta), mk_opts(Event));
handle_event(Event, RpcID, Meta, _Opts) when ?IS_SPAN_END(Event) ->
    span_end(otel_ctx:get_current(), mk_ref(RpcID), fun(SpanCtx) ->
        otel_maybe_erroneous_result(SpanCtx, Meta)
    end);
handle_event(_Event, _RpcID, _Meta, _Opts) ->
    ok.

%%

span_start(Tracer, Ctx, Key, SpanName, Opts) ->
    SpanCtx = otel_tracer:start_span(Ctx, Tracer, SpanName, Opts),
    Ctx1 = woody_util:span_stack_put(Key, SpanCtx, Ctx),
    Ctx2 = otel_tracer:set_current_span(Ctx1, SpanCtx),
    _ = otel_ctx:attach(Ctx2),
    ok.

span_end(Ctx, Key, OnBeforeEnd) ->
    case woody_util:span_stack_pop(Key, Ctx) of
        {error, notfound} ->
            ok;
        {ok, SpanCtx, ParentSpanCtx, Ctx1} ->
            SpanCtx1 = OnBeforeEnd(SpanCtx),
            _ = otel_span:end_span(SpanCtx1, undefined),
            Ctx2 = otel_tracer:set_current_span(Ctx1, ParentSpanCtx),
            _ = otel_ctx:attach(Ctx2),
            ok
    end.

with_span(Ctx, Key, F) ->
    SpanCtx = woody_util:span_stack_get(Key, Ctx, otel_tracer:current_span_ctx(Ctx)),
    _ = F(SpanCtx),
    ok.

otel_maybe_cleanup(#{final := true}, SpanCtx) ->
    _ = otel_span:end_span(SpanCtx, undefined),
    otel_ctx:clear(),
    ok;
otel_maybe_cleanup(_Meta, _SpanCtx) ->
    ok.

otel_maybe_erroneous_result(SpanCtx, #{status := error, result := Reason} = Meta) ->
    Class = maps:get(except_class, Meta, error),
    Stacktrace = maps:get(stack, Meta, []),
    _ = otel_span:record_exception(SpanCtx, Class, Reason, Stacktrace, #{}),
    SpanCtx;
otel_maybe_erroneous_result(SpanCtx, _Meta) ->
    SpanCtx.

mk_opts(?EV_CALL_SERVICE) ->
    #{kind => ?SPAN_KIND_CLIENT};
mk_opts(?EV_INVOKE_SERVICE_HANDLER) ->
    #{kind => ?SPAN_KIND_SERVER}.

mk_ref(#{span_id := WoodySpanId}) ->
    WoodySpanId.

mk_name(#{role := Role, service := Service, function := Function}) ->
    woody_util:to_binary([Role, " ", Service, ":", Function]).
