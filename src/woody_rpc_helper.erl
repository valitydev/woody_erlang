-module(woody_rpc_helper).

-include_lib("opentelemetry_api/include/opentelemetry.hrl").

-export([encode_rpc_context/2]).
-export([decode_rpc_context/1]).

-export_type([t/0]).

-type t() :: map().

-spec encode_rpc_context(woody_context:ctx(), otel_ctx:t()) -> t().
encode_rpc_context(WoodyContext, OtelContext) ->
    #{
        <<"woody">> => woody_context_to_opaque(WoodyContext),
        <<"otel">> => pack_otel_stub(OtelContext)
    }.

-spec decode_rpc_context(t()) -> {woody_context:ctx(), otel_ctx:t()}.
decode_rpc_context(RpcContext) ->
    {decode_woody_context(RpcContext), decode_otel_context(RpcContext)}.

%%

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
