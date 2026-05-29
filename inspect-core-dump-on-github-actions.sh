set -x

container_id=$(docker ps -a --filter='name=cibuildwheel*' -q)
docker start $container_id

# Extract core file
core_file=python3.coredump
sudo coredumpctl dump -o ./$core_file
dest_path="/"
docker cp $core_file $container_id:$dest_path

# Get Python exec file in venv
# coredumpctl shows that the dump comes from another path
# but we want the venv's exec since the venv has the Python client installed
venv_path=$(sudo coredumpctl dump 2>&1 | grep -o -E "/tmp/tmp\..*/venv" | head -n 1)
# GDB already installed in container
docker exec -it -w $dest_path $container_id gdb $venv_path/bin/python3 $core_file
