-module(rpc_thrift_http_transport).

-behaviour(thrift_transport).
-dialyzer(no_undefined_callbacks).

-include("rpc_thrift_http_headers.hrl").

-define(SUPPORTED_TRANSPORT_OPTS, [pool, ssl_options, connect_timeout]).

%% API
-export([new/3]).

-export([start_client_pool/2]).
-export([stop_client_pool/1]).

%% Thrift transport callbacks
-export([read/2, write/2, flush/1, close/1]).


%% Error types and defs
-define(code400, bad_request).
-define(code403, forbidden).
-define(code408, request_timeout).
-define(code413, body_too_large).
-define(code429, too_many_requests).
-define(code500, server_error).
-define(code503, service_unavailable).

-define(transport_error(Reason), {transport_error, Reason}).

-type transport_error(A) :: ?transport_error(A).

-type error_code() ::
    ?code400 | ?code403 | ?code408 | ?code413 | ?code429 |
    ?code500 | ?code503 | {http_code, pos_integer()}.

-type error() ::
    transport_error(error_code())     |
    transport_error(partial_response) |
    transport_error(_).

-export_type([error/0]).

-define(format_error(Error),
    {error, ?transport_error(Error)}
).


-define(log_response(EventHandler, Status, Meta),
    rpc_event_handler:handle_event(EventHandler, 'client receive response',
        Meta#{status =>Status})
).

-type rpc_transport() :: #{
    span_id       => rpc_t:req_id(),
    trace_id      => rpc_t:req_id(),
    parent_id     => rpc_t:req_id(),
    url           => rpc_t:url(),
    options       => map(),
    event_handler => rpc_t:handler(),
    write_buffer  => binary(),
    read_buffer   => binary()
}.


%%
%% API
%%
-spec new(rpc_t:rpc_id(), rpc_client:options(), rpc_t:handler()) ->
    thrift_transport:t_transport() | no_return().
new(RpcId, TransportOpts = #{url := Url}, EventHandler) ->
    TransportOpts1 = maps:remove(url, TransportOpts),
    _ = validate_options(TransportOpts1),
    {ok, Transport} = thrift_transport:new(?MODULE, RpcId#{
        url           => Url,
        options       => TransportOpts1,
        event_handler => EventHandler,
        write_buffer  => <<>>,
        read_buffer   => <<>>
    }),
    Transport.

validate_options(Opts) ->
    BadOpts = maps:without(?SUPPORTED_TRANSPORT_OPTS, Opts),
    map_size(BadOpts) =:= 0 orelse error({badarg, {unsupported_options, BadOpts}}).

-spec start_client_pool(any(), pos_integer()) -> ok.
start_client_pool(Name, Size) ->
    Options = [{max_connections, Size}],
    hackney_pool:start_pool(Name, Options).

-spec stop_client_pool(any()) -> ok | {error, not_found | simple_one_for_one}.
stop_client_pool(Name) ->
    hackney_pool:stop_pool(Name).

%%
%% Thrift transport callbacks
%%
-spec write(rpc_transport(), binary()) -> {rpc_transport(), ok}.
write(Transport = #{write_buffer := WBuffer}, Data) when
    is_binary(WBuffer),
    is_binary(Data)
->
    {Transport#{write_buffer => <<WBuffer/binary, Data/binary>>}, ok}.

-spec read(rpc_transport(), pos_integer()) -> {rpc_transport(), {ok, binary()}}.
read(Transport = #{read_buffer := RBuffer}, Len) when
    is_binary(RBuffer)
->
    Give = min(byte_size(RBuffer), Len),
    <<Data:Give/binary, RBuffer1/binary>> = RBuffer,
    Response = {ok, Data},
    Transport1 = Transport#{read_buffer => RBuffer1},
    {Transport1, Response}.

-spec flush(rpc_transport()) -> {rpc_transport(), ok | {error, error()}}.
flush(Transport = #{
    url           := Url,
    span_id       := SpanId,
    trace_id      := TraceId,
    parent_id     := ParentId,
    options       := Options,
    event_handler := EventHandler,
    write_buffer  := WBuffer,
    read_buffer   := RBuffer
}) when
    is_binary(WBuffer),
    is_binary(RBuffer)
->
    Headers = [
        {<<"content-type">>         , ?CONTENT_TYPE_THRIFT},
        {<<"accept">>               , ?CONTENT_TYPE_THRIFT},
        {?HEADER_NAME_RPC_ROOT_ID   , genlib:to_binary(TraceId)},
        {?HEADER_NAME_RPC_ID        , genlib:to_binary(SpanId)},
        {?HEADER_NAME_RPC_PARENT_ID , genlib:to_binary(ParentId)}
    ],
    RpcId = maps:with([span_id, trace_id, parent_id], Transport),
    rpc_event_handler:handle_event(EventHandler, 'client send request',
        RpcId#{url => Url}),
    case send(Url, Headers, WBuffer, Options, RpcId, EventHandler) of
        {ok, Response} ->
            {Transport#{
                read_buffer  => <<RBuffer/binary, Response/binary>>,
                write_buffer => <<>>
            }, ok};
        Error ->
            {Transport#{read_buffer => <<>>, write_buffer => <<>>}, Error}
    end.

-spec close(rpc_transport()) -> {rpc_transport(), ok}.
close(Transport) ->
    {Transport#{}, ok}.


%%
%% Internal functions
%%
send(Url, Headers, WBuffer, Options, RpcId, EventHandler) ->
    case hackney:request(post, Url, Headers, WBuffer, maps:to_list(Options)) of
        {ok, ResponseCode, _ResponseHeaders, Ref} ->
            ?log_response(EventHandler, get_response_status(ResponseCode),
                RpcId#{code => ResponseCode}),
            handle_response(ResponseCode, hackney:body(Ref));
        {error, {closed, _}} ->
            ?log_response(EventHandler, error,
                RpcId#{reason => partial_response}),
            ?format_error(partial_response);
        {error, Reason} ->
            ?log_response(EventHandler, error,
                RpcId#{reason => Reason}),
            ?format_error(Reason)
    end.

get_response_status(200) -> ok;
get_response_status(_)   -> error.

handle_response(200, {ok, Body}) ->
    {ok, Body};
handle_response(400, _) ->
    ?format_error(?code400);
handle_response(403, _) ->
    ?format_error(?code403);
handle_response(408, _) ->
    ?format_error(?code408);
handle_response(413, _) ->
    ?format_error(?code413);
handle_response(429, _) ->
    ?format_error(?code429);
handle_response(500, _) ->
    ?format_error(?code500);
handle_response(503, _) ->
    ?format_error(?code503);
handle_response(Code, _) ->
    ?format_error({http_code, Code}).
