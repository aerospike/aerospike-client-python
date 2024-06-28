container_name=$1

while true; do
    return_code=$(docker exec $container_name asinfo -v status | grep -qE "^ok")
    if [[ $return_code -eq 0 ]]; then
        # Server is ready when asinfo returns ok
        break
    fi
done
