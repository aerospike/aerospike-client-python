set -x

container_id=$(docker ps -a --filter='name=cibuildwheel*' -q)
docker start $container_id

core_file=python3.coredump
coredumpctl dump -o ./$core_file python3
dest_path="/"
docker cp $core_file $container_id:$dest_path
docker exec $container_id apt update
docker exec $container_id apt install -y gdb
docker exec -w $dest_path $container_id gdb python3 $core_file
