set pagination off
set trace-commands on
set breakpoint pending on

break as_socket_create_fd
command $bpnum
python gdb.execute("finish"); gdb.execute("p fd"); gdb.execute("bt 10"); gdb.execute("continue")
end

# Break when a command socket times out and a connection is added to recover queue
b as_command.c:834
command $bpnum
bt 10
p socket
c
end

# Track when booleans are set that cause connection to be aborted while draining
b as_conn_recover.c:222
commands
watch -l must_abort
commands
c
end
watch -l timeout_exception
commands
c
end
c
end

b as_socket_close
command $bpnum
bt 10 
p sock->fd
c 
end

info b

r
q
