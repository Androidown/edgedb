import os
import urllib.parse
import asyncio
from typing import Tuple, Dict

from edb.server.ha import base as ha_base
from edb.server import pgconnparams
from edb.server.pgcluster import RemoteCluster


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

    async def query_pg_conf(self) -> Dict[str, str]:
        conn = None
        try:
            conn = await self.connect()
            conf_records = await conn.fetch("SELECT * FROM pg_config")
            return dict(rec.values() for rec in conf_records)
        finally:
            if conn is not None:
                await conn.close()
            await asyncio.sleep(0)


def print_conf(conf):
    for k, v in conf.items():
        print(f"{k} = {v}")


async def get_config():
    dsn = os.getenv('_EDGEDB_PG_BACKEND_DSN')
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
