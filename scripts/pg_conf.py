import os
import urllib.parse
import asyncio
from typing import Tuple, Dict
import sys
from contextlib import contextmanager

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.append(ROOT)

from edb.pgsql import params
from edb.server.ha import base as ha_base
from edb.server import pgconnparams
from edb.server.pgcluster import RemoteCluster


@contextmanager
def patch_get_default_runtime_params():
    ori_get_default_runtime_params = params.get_default_runtime_params

    def fake(*args, **kwargs):
        return

    try:
        params.get_default_runtime_params = fake
        yield
    finally:
        params.get_default_runtime_params = ori_get_default_runtime_params
    return


class FakeBackendRuntimeParams:
    has_create_role = False
    session_authorization_role = False


class LazyVersionedCluster(RemoteCluster):
    def __init__(
        self,
        addr: Tuple[str, int],
        params: pgconnparams.ConnectionParameters,
    ):
        self._connection_addr = addr
        self._connection_params = params
        self._ha_backend = None

        self._default_session_auth = None
        self._pg_config_data: Dict[str, str] = {}
        self._pg_bin_dir = None

    def get_runtime_params(self):
        return FakeBackendRuntimeParams()

    async def connect(self, **kwargs):

        with patch_get_default_runtime_params():
            from edb.server import pgcon

        conn_info = self.get_connection_spec()
        conn_info.update(kwargs)
        dbname = conn_info.get("database") or conn_info.get("user")
        assert isinstance(dbname, str)
        return await pgcon.connect(
            conn_info,
            dbname=dbname,
            backend_params=self.get_runtime_params(),
            apply_init_script=False,
        )

    async def query_pg_conf(self) -> Dict[str, str]:
        conn = None
        try:
            conn = await self.connect()
            conf_records = await conn.sql_fetch(b"SELECT * FROM pg_config")
            return dict((k.decode(), v.decode()) for k, v in conf_records)
        finally:
            if conn is not None:
                await conn.close()
            await asyncio.sleep(0)


def print_conf(conf):
    for k, v in conf.items():
        print(f"{k} = {v}")


async def get_config():
    dsn = os.getenv('_EDGEDB_PG_BACKEND_DSN', 'postgresql://postgres:@127.0.0.1:5432?database=postgres')
    parsed = urllib.parse.urlparse(dsn)

    if parsed.scheme not in {'postgresql', 'postgres'}:
        ha_backend = ha_base.get_backend(parsed)
        if ha_backend is None:
            raise ValueError(
                'invalid DSN: scheme is expected to be "postgresql", '
                '"postgres" or one of the supported HA backend, '
                'got {!r}'.format(parsed.scheme))

        addr = await ha_backend.get_cluster_consensus()
        dsn = 'postgresql://{}:{}'.format(*addr)

    addrs, params = pgconnparams.parse_dsn(dsn)
    if len(addrs) > 1:
        raise ValueError('multiple hosts in Postgres DSN are not supported')

    cluster = LazyVersionedCluster(addrs[0], params)

    conf = await cluster.query_pg_conf()
    print_conf(conf)


asyncio.run(get_config())
