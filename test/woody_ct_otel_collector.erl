-module(woody_ct_otel_collector).

-behaviour(gen_server).

-export([
    start_link/0,
    get_trace/1,
    get_traces/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("opentelemetry/include/otel_span.hrl").

-type span() :: #span{}.

-type span_node() :: #{span := span(), children := [span_node()]}.

-type trace() :: #{
    id := opentelemetry:trace_id(),
    node := span_node()
}.

%

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_trace(opentelemetry:trace_id()) -> {ok, trace()} | {error, notfound}.
get_trace(TraceId) ->
    gen_server:call(?MODULE, {trace, TraceId}).

-spec get_traces() -> {ok, [trace()]}.
get_traces() ->
    gen_server:call(?MODULE, traces).

-spec init(_) -> {ok, _}.
init(_Opts) ->
    {ok, #{}}.

-spec handle_info(_, T) -> {noreply, T}.
handle_info({span, Span}, State0) ->
    State1 = maps:update_with(Span#span.trace_id, fun(V) -> [Span | V] end, [Span], State0),
    {noreply, State1};
handle_info(_Msg, State) ->
    {noreply, State}.

-spec handle_call(_, _, T) -> {noreply, T}.
handle_call(traces, _From, State) ->
    Result = maps:map(fun(TraceId, Spans) -> build_trace(TraceId, Spans) end, State),
    {reply, maps:values(Result), State};
handle_call({trace, TraceId}, _From, State) ->
    Result =
        case maps:get(TraceId, State, undefined) of
            undefined -> {error, notfound};
            Spans -> {ok, build_trace(TraceId, Spans)}
        end,
    {reply, Result, State};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(_, T) -> {noreply, T}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%

build_trace(TraceId, Spans0) ->
    Spans1 = lists:sort(fun(#span{start_time = A}, #span{start_time = B}) -> A < B end, Spans0),
    [RootSpan | _] = lists:filter(
        fun
            (#span{parent_span_id = undefined}) -> true;
            (_) -> false
        end,
        Spans1
    ),
    #{
        id => TraceId,
        node => lists:foldl(fun(Span, RootNode) -> update_node(Span, RootNode) end, new_span_node(RootSpan), Spans1)
    }.

update_node(
    #span{parent_span_id = ParentId} = Span,
    #{span := #span{span_id = ParentId}, children := Children} = SpanNode
) ->
    SpanNode#{children => [new_span_node(Span) | Children]};
update_node(Span, #{children := Children} = SpanNode) ->
    SpanNode#{children => lists:map(fun(Child) -> update_node(Span, Child) end, Children)}.

new_span_node(Span) ->
    #{span => Span, children => []}.
