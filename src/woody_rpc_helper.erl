-module(woody_rpc_helper).

%% TODO Add unit testcases to assert encode/decode and viable otel context selection

-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-export([encode_rpc_context/2]).
-export([decode_rpc_context/1]).
-export([attach_otel_context/1]).

-export_type([rpc_context/0]).

-type rpc_context() :: map().

-spec encode_rpc_context(woody_context:ctx(), otel_ctx:t()) -> rpc_context().
encode_rpc_context(WoodyContext, OtelContext) ->
    #{
        <<"woody">> => woody_context_to_opaque(WoodyContext),
        <<"otel">> => pack_otel_stub(OtelContext)
    }.

-spec decode_rpc_context(rpc_context()) -> {woody_context:ctx(), otel_ctx:t()}.
decode_rpc_context(RpcContext) ->
    {decode_woody_context(RpcContext), decode_otel_context(RpcContext)}.

-spec attach_otel_context(otel_ctx:t()) -> ok.
attach_otel_context(OtelContext) when is_map(OtelContext) andalso map_size(OtelContext) =:= 0 ->
    ok;
attach_otel_context(OtelContext) when is_map(OtelContext) ->
    _ = otel_ctx:attach(choose_viable_otel_ctx(OtelContext, otel_ctx:get_current())),
    ok;
attach_otel_context(_) ->
    ok.

%%

%% lowest bit flags if span is sampled
-define(IS_NOT_SAMPLED(SpanCtx), SpanCtx#span_ctx.trace_flags band 2#1 =/= 1).

choose_viable_otel_ctx(NewCtx, CurrentCtx) ->
    case {otel_tracer:current_span_ctx(NewCtx), otel_tracer:current_span_ctx(CurrentCtx)} of
        {#span_ctx{trace_id = TraceID}, #span_ctx{trace_id = TraceID}} ->
            %% If both context belong to same trace, then we use one currently
            %% attached to erlang process.
            CurrentCtx;
        {NewSpanCtx = #span_ctx{}, #span_ctx{}} when ?IS_NOT_SAMPLED(NewSpanCtx) ->
            %% If new context's span is not sampled, then we choose current OTel
            %% context.
            CurrentCtx;
        {undefined, #span_ctx{}} ->
            %% If new context is empty, we give preference to current OTel
            %% context.
            CurrentCtx;
        {_, _} ->
            %% In all other cases we want provided OTel context.
            NewCtx
    end.

decode_woody_context(#{<<"woody">> := OpaqueWoodyContext}) ->
    opaque_to_woody_context(OpaqueWoodyContext);
decode_woody_context(_) ->
    woody_context:new().

decode_otel_context(#{<<"otel">> := PackedOtelContext}) ->
    restore_otel_stub(otel_ctx:get_current(), PackedOtelContext);
decode_otel_context(_) ->
    otel_ctx:get_current().

pack_otel_stub(Ctx) ->
    case otel_tracer:current_span_ctx(Ctx) of
        undefined ->
            [];
        #span_ctx{trace_id = TraceID, span_id = SpanID, trace_flags = TraceFlags} ->
            [trace_id_to_binary(TraceID), span_id_to_binary(SpanID), TraceFlags]
    end.

trace_id_to_binary(TraceID) ->
    {ok, EncodedTraceID} = otel_utils:format_binary_string("~32.16.0b", [TraceID]),
    EncodedTraceID.

span_id_to_binary(SpanID) ->
    {ok, EncodedSpanID} = otel_utils:format_binary_string("~16.16.0b", [SpanID]),
    EncodedSpanID.

restore_otel_stub(Ctx, [TraceID, SpanID, TraceFlags]) ->
    SpanCtx = otel_tracer:from_remote_span(binary_to_id(TraceID), binary_to_id(SpanID), TraceFlags),
    otel_tracer:set_current_span(Ctx, SpanCtx);
restore_otel_stub(Ctx, _Other) ->
    Ctx.

binary_to_id(Opaque) when is_binary(Opaque) ->
    binary_to_integer(Opaque, 16).

woody_context_to_opaque(#{rpc_id := RPCID, meta := ContextMeta}) ->
    [1, woody_rpc_id_to_opaque(RPCID), ContextMeta];
woody_context_to_opaque(#{rpc_id := RPCID}) ->
    [1, woody_rpc_id_to_opaque(RPCID)].

woody_rpc_id_to_opaque(#{span_id := SpanID, trace_id := TraceID, parent_id := ParentID}) ->
    [SpanID, TraceID, ParentID].

opaque_to_woody_context([1, RPCID, ContextMeta]) ->
    #{
        rpc_id => opaque_to_woody_rpc_id(RPCID),
        meta => ContextMeta,
        deadline => undefined
    };
opaque_to_woody_context([1, RPCID]) ->
    #{
        rpc_id => opaque_to_woody_rpc_id(RPCID),
        deadline => undefined
    }.

opaque_to_woody_rpc_id([SpanID, TraceID, ParentID]) ->
    #{span_id => SpanID, trace_id => TraceID, parent_id => ParentID}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-type testgen() :: {_ID, fun(() -> _)}.
-spec test() -> _.

-define(IS_SAMPLED, 1).
-define(NOT_SAMPLED, 0).
-define(OTEL_CTX(IsSampled), ?OTEL_CTX(IsSampled, otel_id_generator:generate_trace_id())).
-define(OTEL_CTX(IsSampled, TraceID),
    otel_tracer:set_current_span(
        otel_ctx:new(),
        (otel_tracer_noop:noop_span_ctx())#span_ctx{
            trace_id = TraceID,
            span_id = otel_id_generator:generate_span_id(),
            is_valid = true,
            is_remote = true,
            is_recording = false,
            trace_flags = IsSampled
        }
    )
).

-spec choose_viable_otel_ctx_test_() -> [testgen()].
choose_viable_otel_ctx_test_() ->
    A = ?OTEL_CTX(?IS_SAMPLED),
    B = ?OTEL_CTX(?NOT_SAMPLED),
    TraceID = otel_id_generator:generate_trace_id(),
    C1 = ?OTEL_CTX(?IS_SAMPLED, TraceID),
    C2 = ?OTEL_CTX(?IS_SAMPLED, TraceID),
    [
        ?_assertEqual(A, choose_viable_otel_ctx(A, B)),
        ?_assertEqual(A, choose_viable_otel_ctx(B, A)),
        ?_assertEqual(A, choose_viable_otel_ctx(A, otel_ctx:new())),
        ?_assertEqual(B, choose_viable_otel_ctx(otel_ctx:new(), B)),
        ?_assertEqual(otel_ctx:new(), choose_viable_otel_ctx(otel_ctx:new(), otel_ctx:new())),
        ?_assertNotEqual(C1, C2),
        ?_assertEqual(C1, choose_viable_otel_ctx(C2, C1)),
        ?_assertEqual(C2, choose_viable_otel_ctx(C1, C2))
    ].

-endif.
