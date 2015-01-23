
# aerospike.Client.info_node

aerospike.Client.info_node - sends an info request to a single cluster node.

## Description

```
response = aerospike.Client.info_node( request , host , policies )

```

**aerospike.Client.info_node()** returns a *response* for a particular *request* string

## Parameters

**request**, a string representing a command and control operation.

**host**, the dictionary specifying the host.

**policies**, the dictionary of policies to be given while retreiving information about a particular host. 

**[policies](aerospike.md)** including,    
- **timeout**

## Return Values
Returns a server response for the particular request string.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connect()

request = "statistics"
host = {"addr": "127.0.0.1", "port": 3000}   
response = client.info_node( request, host )

print response

```

We expect to see:

```python
{'BB9EA1125270008': (None, 'cluster_size=1;cluster_key=AEB5879C5355C96;cluster_integrity=true;objects=12;total-bytes-disk=0;used-bytes-disk=0;free-pct-disk=0;total-bytes-memory=8589934592;used-bytes-memory=38022;data-used-bytes-memory=204;index-used-bytes-memory=768;sindex-used-bytes-memory=37050;free-pct-memory=99;stat_read_reqs=413;stat_read_reqs_xdr=0;stat_read_success=397;stat_read_errs_notfound=16;stat_read_errs_other=0;stat_write_reqs=3982;stat_write_reqs_xdr=0;stat_write_success=3683;stat_write_errs=100;stat_xdr_pipe_writes=0;stat_xdr_pipe_miss=0;stat_delete_success=1272;stat_rw_timeout=0;udf_read_reqs=199;udf_read_success=8;udf_read_errs_other=191;udf_write_reqs=182;udf_write_success=182;udf_write_err_others=0;udf_delete_reqs=0;udf_delete_success=0;udf_delete_err_others=0;udf_lua_errs=216;udf_scan_rec_reqs=557;udf_query_rec_reqs=557;udf_replica_writes=0;stat_proxy_reqs=0;stat_proxy_reqs_xdr=0;stat_proxy_success=0;stat_proxy_errs=0;stat_cluster_key_trans_to_proxy_retry=0;stat_cluster_key_transaction_reenqueue=0;stat_slow_trans_queue_push=0;stat_slow_trans_queue_pop=0;stat_slow_trans_queue_batch_pop=0;stat_cluster_key_regular_processed=0;stat_cluster_key_prole_retry=0;stat_cluster_key_err_ack_dup_trans_reenqueue=0;stat_cluster_key_partition_transaction_queue_count=0;stat_cluster_key_err_ack_rw_trans_reenqueue=0;stat_expired_objects=0;stat_evicted_objects=0;stat_deleted_set_objects=0;stat_evicted_set_objects=0;stat_evicted_objects_time=0;stat_zero_bin_records=4;stat_nsup_deletes_not_shipped=0;err_tsvc_requests=116;err_out_of_space=0;err_duplicate_proxy_request=0;err_rw_request_not_found=0;err_rw_pending_limit=0;err_rw_cant_put_unique=0;fabric_msgs_sent=32794;fabric_msgs_rcvd=32793;paxos_principal=BB9EA1125270008;migrate_msgs_sent=16398;migrate_msgs_recv=32783;migrate_progress_send=0;migrate_progress_recv=0;migrate_num_incoming_accepted=8192;migrate_num_incoming_refused=0;queue=0;transactions=13384;reaped_fds=0;tscan_initiate=305;tscan_pending=0;tscan_succeeded=88;tscan_aborted=0;batch_initiate=32;batch_queue=0;batch_tree_count=0;batch_timeout=0;batch_errors=0;info_queue=0;proxy_initiate=0;proxy_action=0;proxy_retry=0;proxy_retry_q_full=0;proxy_unproxy=0;proxy_retry_same_dest=0;proxy_retry_new_dest=0;write_master=3982;write_prole=0;read_dup_prole=0;rw_err_dup_internal=0;rw_err_dup_cluster_key=0;rw_err_dup_send=0;rw_err_write_internal=0;rw_err_write_cluster_key=0;rw_err_write_send=0;rw_err_ack_internal=0;rw_err_ack_nomatch=0;rw_err_ack_badnode=0;client_connections=4;waiting_transactions=0;tree_count=0;record_refs=12;record_locks=0;migrate_tx_objs=0;migrate_rx_objs=0;ongoing_write_reqs=0;err_storage_queue_full=0;partition_actual=8192;partition_replica=0;partition_desync=0;partition_absent=0;partition_object_count=12;partition_ref_count=8192;system_free_mem_pct=57;sindex_ucgarbage_found=0;sindex_gc_locktimedout=0;sindex_gc_inactivity_dur=7195845;sindex_gc_activity_dur=155;sindex_gc_list_creation_time=61;sindex_gc_list_deletion_time=16;sindex_gc_objects_validated=127;sindex_gc_garbage_found=0;sindex_gc_garbage_cleaned=0;system_swapping=false;err_replica_null_node=0;err_replica_non_null_node=0;err_sync_copy_null_node=0;err_sync_copy_null_master=0;storage_defrag_corrupt_record=0;err_write_fail_prole_unknown=0;err_write_fail_prole_generation=0;err_write_fail_unknown=0;err_write_fail_key_exists=0;err_write_fail_generation=8;err_write_fail_generation_xdr=0;err_write_fail_bin_exists=0;err_write_fail_parameter=4;err_write_fail_incompatible_type=4;err_write_fail_noxdr=0;err_write_fail_prole_delete=0;err_write_fail_not_found=4;err_write_fail_key_mismatch=0;stat_duplicate_operation=0;uptime=7613;stat_write_errs_notfound=84;stat_write_errs_other=16;heartbeat_received_self=50368;heartbeat_received_foreign=753;query_reqs=72;query_success=44;query_fail=28;query_abort=0;query_avg_rec_count=1;query_short_queue_full=0;query_long_queue_full=0;query_short_running=56;query_long_running=0;query_tracked=0;query_agg=28;query_agg_success=16;query_agg_err=12;query_agg_abort=0;query_agg_avg_rec_count=1;query_lookups=28;query_lookup_success=28;query_lookup_err=0;query_lookup_abort=0;query_lookup_avg_rec_count=1\n')}
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)
