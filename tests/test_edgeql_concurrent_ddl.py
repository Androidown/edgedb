#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import asyncio
import contextlib

from edb.testbase import server as tb


async def txn_execute(conn, eql):
    async with conn.transaction():
        await conn.execute(eql)


class TestEdgeQLConcurrentDDL(tb.DDLTestCase):
    @contextlib.asynccontextmanager
    async def aquire_connections(self, n: int):
        assert n > 0
        dbname = self.get_database_name()

        conns = []
        for _ in range(n):
            conns.append(await self.connect(database=dbname))

        try:
            yield conns
        finally:
            for c in conns:
                await c.aclose()

    async def test_concurrent_dll_no_txn(self):
        async with self.aquire_connections(3) as (c0, c1, c2):
            await asyncio.gather(
                c0.execute("CREATE TYPE X"),
                c1.execute("CREATE TYPE Y"),
                c2.execute("CREATE TYPE Z"),
            )

            await asyncio.gather(
                c0.execute("DROP TYPE Z"),
                c1.execute("DROP TYPE X"),
                c2.execute("DROP TYPE Y"),
            )
