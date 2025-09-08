set pagination off
set trace-commands on
set breakpoint pending on

break as_socket_create_fd
command 1
python gdb.execute("finish"); gdb.execute("p fd"); gdb.execute("bt 10"); gdb.execute("continue")
end

b as_socket_close
command 2
bt 10 
p sock->fd
c 
end

info b

r
q
