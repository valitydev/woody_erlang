-module(woody_caching_client).

-include("woody_defs.hrl").

%% API
-export_type([cache_control/0]).
-export_type([cache_options/0]).
-export_type([options/0]).

-export([child_spec/2]).
-export([start_link/1]).
-export([call/3]).
-export([call/4]).

%% Internal API
-export([call_safe/4]).

%%
%% API
%%
-type cache_control() :: cache | {cache_for, TimeoutMs :: non_neg_integer()} | no_cache.

-type cache_options() :: #{
    local_name => atom(),
    type => set | ordered_set,
    policy => lru | mru,
    memory => integer(),
    size => integer(),
    n => integer(),
    %% seconds
    ttl => integer(),
    check => integer(),
    stats => function() | {module(), atom()},
    heir => atom() | pid()
}.

-type options() :: #{
    workers_name := atom(),
    cache := cache_options(),
    woody_client := woody_client:options(),
    joint_control => joint | no_joint
}.

-spec child_spec(atom(), options()) -> supervisor:child_spec().
child_spec(ChildID, Options) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [Options]},
        restart => permanent,
        type => supervisor
    }.

-spec start_link(options()) -> genlib_gen:start_ret().
start_link(Options) ->
    genlib_adhoc_supervisor:start_link(
        #{strategy => one_for_one},
        [
            woody_joint_workers:child_spec(joint_workers, workers_reg_name(Options)),
            cache_child_spec(cache, Options),
            woody_client:child_spec(woody_client_options(Options))
        ]
    ).

-spec call(woody:request(), cache_control(), options()) ->
    {ok, woody:result()}
    | {exception, woody_error:business_error()}
    | no_return().
call(Request, CacheControl, Options) ->
    call(Request, CacheControl, Options, woody_context:new()).

-spec call(woody:request(), cache_control(), options(), woody_context:ctx()) ->
    {ok, woody:result()}
    | {exception, woody_error:business_error()}
    | no_return().
call(Request, CacheControl, #{joint_control := joint} = Options, Context) ->
    Task = fun(_) ->
        call_safe(Request, CacheControl, Options, Context)
    end,
    woody_joint_workers:do(workers_ref(Options), Request, Task, woody_context:get_deadline(Context));
call(Request, CacheControl, Options, Context) ->
    call_safe(Request, CacheControl, Options, Context).

%%
%% Internal API
%%

-spec call_safe(woody:request(), cache_control(), options(), woody_context:ctx()) ->
    {ok, woody:result()}
    | {exception, woody_error:business_error()}.
call_safe(Request, CacheControl, Options, Context) ->
    Meta = add_thrift_meta(Request, new_meta(Options, Context)),
    _ = emit_event(?EV_CLIENT_CACHE_BEGIN, Meta, Context, Options),
    try
        do_call(Request, CacheControl, Options, Context, Meta)
    after
        _ = emit_event(?EV_CLIENT_CACHE_END, Meta, Context, Options)
    end.

-spec do_call(woody:request(), cache_control(), options(), woody_context:ctx(), map()) ->
    {ok, woody:result()}
    | {exception, woody_error:business_error()}.
do_call(Request, CacheControl, Options, Context, Meta) ->
    Result =
        case get_from_cache(Request, CacheControl, Options) of
            OK = {ok, _CacheResult} ->
                % cache hit
                ok = emit_event(?EV_CLIENT_CACHE_HIT, Meta, Context, Options),
                OK;
            not_found ->
                % cache miss
                ok = emit_event(?EV_CLIENT_CACHE_MISS, Meta, Context, Options),
                case woody_client:call(Request, woody_client_options(Options), Context) of
                    {ok, CallResult} ->
                        % cache update
                        ok = emit_event(?EV_CLIENT_CACHE_UPDATE, Meta#{result => CallResult}, Context, Options),
                        ok = update_cache(Request, CallResult, CacheControl, Options),
                        {ok, CallResult};
                    Exception = {exception, _} ->
                        Exception
                end
        end,
    ok = emit_event(?EV_CLIENT_CACHE_RESULT, Meta#{result => Result}, Context, Options),
    Result.

%%
%% local
%%
-spec get_from_cache(_Key, cache_control(), options()) -> not_found | {ok, _Value}.
get_from_cache(_, no_cache, _) ->
    not_found;
get_from_cache(Key, CacheControl, Options) ->
    Now = now_ms(),
    case {CacheControl, cache:get(cache_name(Options), Key)} of
        {_, undefined} ->
            not_found;
        {{cache_for, Lifetime}, {Ts, _}} when Ts + Lifetime < Now ->
            not_found;
        {_, {_, Value}} ->
            {ok, Value}
    end.

-spec update_cache(_Key, _Value, cache_control(), options()) -> ok.
update_cache(_, _, no_cache, _) ->
    ok;
update_cache(Key, Value, cache, Options) ->
    ok = cache:put(cache_name(Options), Key, {now_ms(), Value});
update_cache(Key, Value, {cache_for, LifetimeMs}, Options) ->
    ok = cache:put(cache_name(Options), Key, {now_ms(), Value}, LifetimeMs div 1000).

%%

-spec workers_reg_name(options()) -> genlib_gen:reg_name().
workers_reg_name(#{workers_name := Name}) ->
    {local, Name}.

-spec workers_ref(options()) -> genlib_gen:ref().
workers_ref(#{workers_name := Name}) ->
    Name.

-spec cache_child_spec(atom(), options()) -> supervisor:child_spec().
cache_child_spec(ChildID, Options) ->
    #{
        id => ChildID,
        start => {cache, start_link, [cache_name(Options), cache_options(Options)]},
        restart => permanent,
        type => supervisor
    }.

-spec cache_name(options()) -> atom().
cache_name(#{cache := #{local_name := Name}}) ->
    Name.

-spec cache_options(options()) -> list().
cache_options(#{cache := Options}) ->
    maps:to_list(Options).

-spec woody_client_options(options()) -> woody_client:options().
woody_client_options(#{woody_client := Options}) ->
    Options.

-spec now_ms() -> integer().
now_ms() ->
    % The cache library uses os:timestamp/0 to get the current time, so just do the same
    os:system_time(millisecond).

-spec emit_event(woody_event_handler:event(), map(), woody_context:ctx(), options()) -> ok.
emit_event(Event, Meta, #{rpc_id := RPCID}, Options) ->
    _ = woody_event_handler:handle_event(woody_event_handler(Options), Event, RPCID, Meta),
    ok.

-spec woody_event_handler(options()) -> woody:ev_handlers().
woody_event_handler(#{woody_client := #{event_handler := EventHandler}}) ->
    EventHandler.

-spec url(options()) -> woody:url().
url(#{woody_client := #{url := URL}}) ->
    URL.

-spec new_meta(options(), woody_context:ctx()) -> map().
new_meta(Options, Context) ->
    #{
        role => client,
        metadata => woody_context:get_meta(Context),
        url => url(Options)
    }.

%% FIXME ?????????????????????? ????????????????????
-spec add_thrift_meta(woody:request(), map()) -> map().
add_thrift_meta({Service = {_, ServiceName}, Function, Args}, Meta) ->
    Meta#{
        service => ServiceName,
        service_schema => Service,
        function => Function,
        type => woody_util:get_rpc_type(Service, Function),
        args => Args
    }.
