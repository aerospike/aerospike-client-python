/entrypoint.sh

SUPERUSER_NAME_AND_PASSWORD=superuser
ASADM_AUTH_FLAGS="--user=$SUPERUSER_NAME_AND_PASSWORD --password=$SUPERUSER_NAME_AND_PASSWORD"
asadm $ASADM_AUTH_FLAGS --enable --execute "manage revive ns test"
asadm $ASADM_AUTH_FLAGS --enable --execute "manage recluster"
