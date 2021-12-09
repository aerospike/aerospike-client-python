/*******************************************************************************
 * Copyright 2017-2021 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
#include "tls_info_host.h"
#include <aerospike/as_cluster.h>
#include <aerospike/as_lookup.h>
#include <aerospike/as_info.h>

as_status send_info_to_tls_host(aerospike *as, as_error *err,
								const as_policy_info *info_policy,
								const char *hostname, uint16_t port,
								const char *tls_name, const char *request,
								char **response)
{

	as_status status = AEROSPIKE_OK;
	as_cluster *cluster = as->cluster;
	as_address_iterator iter;

	if (!cluster) {
		return as_error_update(err, AEROSPIKE_ERR_CLUSTER, "Invalid cluster");
	}

	as_lookup_host(&iter, err, hostname, port);

	if (err->code != AEROSPIKE_OK) {
		return err->code;
	}

	struct sockaddr *addr;
	status = AEROSPIKE_ERR_CLUSTER;
	bool loop = true;

	if (!info_policy) {
		info_policy = &as->config.policies.info;
	}
	uint64_t deadline = as_socket_deadline(info_policy->timeout);

	while (loop && as_lookup_next(&iter, &addr)) {
		status = as_info_command_host(cluster, err, addr, (char *)request,
									  info_policy->send_as_is, deadline,
									  response, tls_name);

		switch (status) {
		case AEROSPIKE_OK:
		case AEROSPIKE_ERR_TIMEOUT:
		case AEROSPIKE_ERR_INDEX_FOUND:
		case AEROSPIKE_ERR_INDEX_NOT_FOUND:
			loop = false;
			break;

		default:
			break;
		}
	}
	as_lookup_end(&iter);
	return status;
}
