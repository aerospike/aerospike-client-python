asadm -h aerospike -U admin -P admin --enable --execute "manage acl grant role user-admin priv service-ctrl"
asadm -h aerospike -U admin -P admin --enable --execute "manage revive ns test"
asadm -h aerospike -U admin -P admin --enable --execute "manage recluster"
