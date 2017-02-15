-module(woody_client_behaviour).

-export([call/3]).

%% Behaviour definition
-callback call(woody:request(), woody_client:options(), woody_context:ctx()) ->  woody_client:result().

-spec call(woody:request(), woody_client:options(), woody_context:ctx()) ->
    woody_client:result().
call(Request, Options, Context) ->
    Handler = woody_util:get_protocol_handler(client, Options),
    Handler:call(Request, Options, Context).