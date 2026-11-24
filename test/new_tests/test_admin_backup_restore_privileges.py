# -*- coding: utf-8 -*-

import time
from typing import Any

import pytest

import aerospike
from aerospike import exception as e

from .test_base_class import TestBaseClass

# Maps each constant's name to the privilege code the server uses for it.
BACKUP_RESTORE_PRIVILEGES: list[tuple[str, int]] = [
    ("PRIV_BACKUP_ADMIN", 5),
    ("PRIV_RESTORE_ADMIN", 6),
]


class TestBackupRestorePrivileges(object):
    """
    These privileges require server 8.2.1+. In server 8.2.1, the server only accepts them when it is started with
    --preview backup-restore, so the tests that send them to the server skip if the server refuses them.
    """

    config = TestBaseClass.get_connection_config()

    pytestmark = pytest.mark.skipif(
        not TestBaseClass.auth_in_use(), reason="No user specified, may be not secured cluster."
    )

    ROLE_NAME: str = "backup-restore-priv-test"
    # Codes below PRIV_READ are global, so the server returns them with no namespace or set
    GLOBAL_PRIVILEGES: list[dict[str, Any]] = [
        {"code": aerospike.PRIV_USER_ADMIN, "ns": "", "set": ""},
        {"code": aerospike.PRIV_SYS_ADMIN, "ns": "", "set": ""},
    ]

    def setup_method(self, method: Any) -> None:
        config = self.config
        self.client = aerospike.client(config).connect(config["user"], config["password"])
        self.drop_role_if_exists()

    def teardown_method(self, method: Any) -> None:
        self.drop_role_if_exists()
        self.client.close()

    def drop_role_if_exists(self) -> None:
        try:
            self.client.admin_drop_role(self.ROLE_NAME)
            time.sleep(1)
        except e.InvalidRole:
            pass  # we are good, no such role exists

    def skip_because_server_refuses_privilege(self, err: e.InvalidPrivilege) -> None:
        reason = f"Server refused the privilege ({err}). Requires server 8.2.1+ started with --preview backup-restore"
        pytest.skip(reason=reason)

    @pytest.mark.parametrize("priv_name, priv_code", BACKUP_RESTORE_PRIVILEGES)
    def test_create_role_positive(self, priv_name: str, priv_code: int) -> None:
        priv_constant = getattr(aerospike, priv_name)
        try:
            self.client.admin_create_role(self.ROLE_NAME, [{"code": priv_constant}])
        except e.InvalidPrivilege as err:
            self.skip_because_server_refuses_privilege(err)
        time.sleep(1)

        # The server must see the code we expect, not just whatever the constant happens to be
        expected_privileges = [{"code": priv_code, "ns": "", "set": ""}]
        privileges = self.client.admin_query_role(self.ROLE_NAME)
        assert privileges == expected_privileges

        role = self.client.admin_get_role(self.ROLE_NAME)
        assert role == {"privileges": expected_privileges, "whitelist": [], "read_quota": 0, "write_quota": 0}

    @pytest.mark.parametrize("priv_name, priv_code", BACKUP_RESTORE_PRIVILEGES)
    def test_grant_and_revoke_privileges_positive(self, priv_name: str, priv_code: int) -> None:
        priv_constant = getattr(aerospike, priv_name)
        self.client.admin_create_role(self.ROLE_NAME, self.GLOBAL_PRIVILEGES)
        time.sleep(1)

        try:
            status = self.client.admin_grant_privileges(self.ROLE_NAME, [{"code": priv_constant}])
        except e.InvalidPrivilege as err:
            self.skip_because_server_refuses_privilege(err)
        assert status == 0
        time.sleep(1)

        granted_privilege = {"code": priv_code, "ns": "", "set": ""}
        privileges = self.client.admin_query_role(self.ROLE_NAME)
        assert privileges == [*self.GLOBAL_PRIVILEGES, granted_privilege]

        status = self.client.admin_revoke_privileges(self.ROLE_NAME, [{"code": priv_constant}])
        assert status == 0
        time.sleep(1)

        privileges = self.client.admin_query_role(self.ROLE_NAME)
        assert privileges == self.GLOBAL_PRIVILEGES

    @pytest.mark.parametrize("priv_name, priv_code", BACKUP_RESTORE_PRIVILEGES)
    def test_grant_privilege_with_namespace_scope_negative(self, priv_name: str, priv_code: int) -> None:
        # The client rejects this before it reaches the server, so this doesn't depend on the server's version
        priv_constant = getattr(aerospike, priv_name)
        self.client.admin_create_role(self.ROLE_NAME, self.GLOBAL_PRIVILEGES)
        time.sleep(1)

        with pytest.raises(e.ParamError, match="namespace/set scope which is invalid"):
            self.client.admin_grant_privileges(self.ROLE_NAME, [{"code": priv_constant, "ns": "test"}])
