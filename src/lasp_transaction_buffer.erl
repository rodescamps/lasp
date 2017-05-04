
-module(lasp_transaction_buffer).
-author("Robin Descamps <robin.descamps@student.uclouvain.be>").
% From a counter example found here: http://erlang.org/pipermail/erlang-questions/2009-June/044893.html

-export([new/0,
		 get_pid/0,
		 addToBuffer/2,
		 deleteFromBuffer/2,
		 get/1]).
-include("lasp.hrl").

%% ====================================================================
%% Internal functions
%% ====================================================================
 
% Returns the Pid of this process
get_pid() ->
    case whereis(buffer_pid) of 
        undefined -> register(buffer_pid, lasp_transaction_buffer:new()), get_pid();
        Pid -> Pid
    end.

new() ->
	spawn(fun () -> loop({maps:new(), 1}) end).

% [Ensure atomicity and FIFO]
% Size = Buffer size
% N = the sequence number we want, to ensure FIFO order
loop({Buffer, Size}) ->
	receive {Msg,Sender} ->
		Sender ! {self(), Buffer},
			loop(case Msg
 		       of {add, Node, Op} -> {maps:put({Node, Size}, Op, Buffer), Size+1}
 		        ; {delete, {Node, N}} -> {maps:remove({Node, N}, Buffer), Size}
				; get -> {Buffer, Size}
 		     end)
 	    end.

addToBuffer(Buffer, Op) ->
	{ok, Nodes} = ?SYNC_BACKEND:membership(),
	lists:foreach(fun(Node) -> add(Buffer, Node, Op) end, Nodes).

% Helper to add a new operation to all of the nodes in the cluster
add(Buffer, Node, Op) ->
	do(Buffer, {add, Node, Op}).

deleteFromBuffer(Buffer, {Node, N}) ->
	do(Buffer, {delete, {Node, N}}).

get(Buffer) ->
	do(Buffer, get).
 
do(Pid, Msg) when is_pid(Pid) ->
	Pid ! {Msg,self()},
 	receive {Pid,Result} -> Result end.
