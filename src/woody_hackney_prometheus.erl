%% Translates hackney metrics for prometheus.
%% See
%% https://github.com/benoitc/hackney/tree/1.18.0?tab=readme-ov-file#metrics
%%
%% For pool metrics see dedicated collector module
%% `woody_hackney_prometheus_collector`.
-module(woody_hackney_prometheus).

-export([
    new/2,
    delete/1,
    increment_counter/1,
    increment_counter/2,
    decrement_counter/1,
    decrement_counter/2,
    update_histogram/2,
    update_gauge/2,
    update_meter/2
]).

-type name() :: any().
-type metric() :: counter | histogram | gauge | meter.

%% API

-define(NB_REQUESTS, hackney_nb_requests).
-define(TOTAL_REQUESTS, hackney_total_requests).
-define(HOST_NB_REQUESTS, hackney_host_nb_requests).
-define(HOST_REQUEST_TIME, hackney_host_request_time).
-define(HOST_CONNECT_TIMEOUT, hackney_host_connect_timeout).
-define(HOST_CONNECT_ERROR, hackney_host_connect_error).
-define(HOST_NEW_CONNECTION, hackney_host_new_connection).
-define(HOST_REUSE_CONNECTION, hackney_host_reuse_connection).

%% Ignore unsupported metric
-spec new(metric(), name()) -> ok.
%% Total counters
new(counter, [hackney, nb_requests]) ->
    true = prometheus_gauge:declare([
        {name, ?NB_REQUESTS},
        {registry, registry()},
        {labels, []},
        {help, "Number of running requests."}
    ]),
    %% Per host counters, see
    %% https://github.com/benoitc/hackney/tree/1.18.0?tab=readme-ov-file#metrics-per-hosts
    %% NOTE Hackney won't call `metrics:new/3` for those counters
    true = prometheus_gauge:declare([
        {name, ?HOST_NB_REQUESTS},
        {registry, registry()},
        {labels, [host]},
        {help, "Number of running requests."}
    ]),
    true = prometheus_histogram:declare([
        {name, ?HOST_REQUEST_TIME},
        {registry, registry()},
        {labels, [host]},
        {buckets, request_time_buckets_ms()},
        {help, "Request time."}
    ]),
    [
        true = prometheus_counter:declare([
            {name, Name},
            {registry, registry()},
            {labels, [host]},
            {help, Help}
        ])
     || {Name, Help} <- [
            {?HOST_CONNECT_TIMEOUT, "Number of connect timeout."},
            {?HOST_CONNECT_ERROR, "Number of timeout errors."},
            {?HOST_NEW_CONNECTION, "Number of new pool connections per host."},
            {?HOST_REUSE_CONNECTION, "Number of reused pool connections per host."}
        ]
    ],
    ok;
new(counter, [hackney, total_requests]) ->
    true = prometheus_counter:declare([
        {name, ?TOTAL_REQUESTS},
        {registry, registry()},
        {labels, []},
        {help, "Total number of requests."}
    ]),
    ok;
new(_Type, _Name) ->
    ok.

-spec delete(name()) -> ok.
delete(_Name) ->
    ok.

-spec increment_counter(name()) -> ok.
increment_counter(Name) ->
    increment_counter(Name, 1).

-spec increment_counter(name(), pos_integer()) -> ok.
increment_counter([hackney, nb_requests], Value) ->
    prometheus_gauge:inc(registry(), ?NB_REQUESTS, [], Value);
increment_counter([hackney, total_requests], Value) ->
    prometheus_counter:inc(registry(), ?TOTAL_REQUESTS, [], Value);
increment_counter([hackney, Host, nb_requests], Value) ->
    prometheus_gauge:inc(registry(), ?HOST_NB_REQUESTS, [Host], Value);
increment_counter([hackney, Host, connect_timeout], Value) ->
    prometheus_counter:inc(registry(), ?HOST_CONNECT_TIMEOUT, [Host], Value);
increment_counter([hackney, Host, connect_error], Value) ->
    prometheus_counter:inc(registry(), ?HOST_CONNECT_ERROR, [Host], Value);
increment_counter([hackney_pool, Host, new_connection], Value) ->
    prometheus_counter:inc(registry(), ?HOST_NEW_CONNECTION, [Host], Value);
increment_counter([hackney_pool, Host, reuse_connection], Value) ->
    prometheus_counter:inc(registry(), ?HOST_REUSE_CONNECTION, [Host], Value);
increment_counter(_Name, _Value) ->
    ok.

-spec decrement_counter(name()) -> ok.
decrement_counter(Name) ->
    decrement_counter(Name, 1).

-spec decrement_counter(name(), pos_integer()) -> ok.
decrement_counter([hackney, nb_requests], Value) ->
    prometheus_gauge:dec(registry(), ?NB_REQUESTS, [], Value);
decrement_counter([hackney, Host, nb_requests], Value) ->
    prometheus_gauge:dec(registry(), ?HOST_NB_REQUESTS, [Host], Value);
decrement_counter(_Name, _Value) ->
    ok.

-spec update_histogram(name(), fun(() -> ok) | number()) -> ok.
update_histogram(_Name, Fun) when is_function(Fun, 0) ->
    Fun();
update_histogram([hackney, Host, request_time], Value) ->
    _ = prometheus_histogram:observe(registry(), ?HOST_REQUEST_TIME, [Host], Value),
    ok;
update_histogram(_Name, _Value) ->
    ok.

-spec update_gauge(name(), number()) -> ok.
update_gauge(_Name, _Value) ->
    ok.

-spec update_meter(name(), number()) -> ok.
update_meter(_Name, _Value) ->
    ok.

%%

registry() ->
    default.

request_time_buckets_ms() ->
    [
        5,
        10,
        25,
        50,
        100,
        250,
        500,
        1000,
        10000
    ].
