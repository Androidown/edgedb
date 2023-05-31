import json

import edgedb

from edb.schema import defines as s_def
from edb.testbase import server as tb


class TestNameSpace(tb.DatabaseTestCase):
    TRANSACTION_ISOLATION = False

    async def assert_query_in_conn(
        self, conn, query, exp_result
    ):
        res = await conn.query_json(query)
        self.assertEqual(json.loads(res), exp_result)

    async def test_create_drop_namespace(self):
        await self.con.execute("create namespace ns1;")
        await self.assert_query_result(
            r"select sys::NameSpace{name} order by .name",
            [{'name': s_def.DEFAULT_NS}, {'name': 'ns1'}]
        )
        await self.con.execute("drop namespace ns1;")
        await self.assert_query_result(
            r"select sys::NameSpace{name} order by .name",
            [{'name': s_def.DEFAULT_NS}]
        )

    async def test_create_namespace_invalid(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'NameSpace names can not be started with \'pg_\', '
                f'as such names are reserved for system schemas',
        ):
            await self.con.execute("create namespace pg_ns1;")

        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                f'\'{s_def.DEFAULT_NS}\' is reserved as name for '
                f'default namespace, use others instead.'
        ):
            await self.con.execute(f"create namespace {s_def.DEFAULT_NS};")

    async def test_create_namespace_exists(self):
        await self.con.execute("create namespace ns2;")

        with self.assertRaisesRegex(
                edgedb.EdgeDBError,
                'namespace "ns2" already exists',
        ):
            await self.con.execute("create namespace ns2;")

        await self.con.execute("drop namespace ns2;")

    async def test_drop_namespace_invalid(self):
        with self.assertRaisesRegex(
                edgedb.EdgeDBError,
                'namespace "ns3" does not exist',
        ):
            await self.con.execute("drop namespace ns3;")

        with self.assertRaisesRegex(
                edgedb.ExecutionError,
                f"namespace '{s_def.DEFAULT_NS}' cannot be dropped",
        ):
            await self.con.execute(f"drop namespace {s_def.DEFAULT_NS};")

        await self.con.execute("create namespace n1;")
        await self.con.execute("use namespace n1;")
        with self.assertRaisesRegex(
                edgedb.ExecutionError,
                f"cannot drop the currently open current_namespace 'n1'",
        ):
            await self.con.execute(f"drop namespace n1;")

    async def test_use_show_namespace(self):
        await self.con.execute("create namespace temp1;")
        # check default
        conn1 = await self.connect(database=self.get_database_name())
        conn2 = await self.connect(database=self.get_database_name())
        try:
            await self.assert_query_in_conn(conn1, 'show namespace;', [s_def.DEFAULT_NS])
            await self.assert_query_in_conn(conn2, 'show namespace;', [s_def.DEFAULT_NS])

            # check seperated between connection
            await conn1.execute('use namespace temp1;')
            await self.assert_query_in_conn(conn1, 'show namespace;', ['temp1'])
            await self.assert_query_in_conn(conn2, 'show namespace;', [s_def.DEFAULT_NS])

            # check use
            await conn1.execute(
                'CONFIGURE SESSION SET __internal_testmode := true;'
                'create type A;'
                'CONFIGURE SESSION SET __internal_testmode := false;'
            )

            await self.assert_query_in_conn(
                conn1,
                'select count((select schema::ObjectType filter .name="default::A"))',
                [1]
            )

            await self.assert_query_in_conn(
                conn2,
                'select count((select schema::ObjectType filter .name="default::A"))',
                [0]
            )

            await conn2.execute('drop namespace temp1;')

            with self.assertRaises(edgedb.QueryError):
                await conn1.query("select 1")

            await self.assert_query_in_conn(conn1, 'show namespace;', [s_def.DEFAULT_NS])
        finally:
            await conn1.aclose()
            await conn2.aclose()

    async def test_use_namespace_invalid(self):
        await self.con.execute("create namespace ns4;")
        try:
            with self.assertRaises(edgedb.QueryError):
                await self.con.execute("use namespace ns5;")

            with self.assertRaisesRegex(
                    edgedb.ProtocolError,
                    'USE NAMESPACE statement is not allowed to be used in script.',
            ):
                await self.con.execute("use namespace ns4;select 1;")

            await self.con.execute("START TRANSACTION")

            with self.assertRaisesRegex(
                    edgedb.ProtocolError,
                    'USE NAMESPACE statement is not allowed to be used in transaction.',
            ):
                await self.con.execute("use namespace ns4;")

            await self.con.execute("ROLLBACK")

        finally:
            await self.con.execute("drop namespace ns4;")

    async def test_concurrent_schema_version_change_between_ns(self):
        await self.con.execute("create namespace temp1;")
        await self.con.execute("create namespace temp2;")

        conn1 = await self.connect(database=self.get_database_name())
        conn2 = await self.connect(database=self.get_database_name())

        try:
            await conn1.execute('use namespace temp1;')
            await conn2.execute('use namespace temp2;')

            await conn1.execute(
                '''
                START MIGRATION TO {
                    module default {
                        type A5;
                        type Object5 {
                            required link a -> default::A5;
                        };
                    };
                };
                '''
            )
            await conn1.execute('POPULATE MIGRATION')
            async with conn2.transaction():
                await conn2.execute(
                    '''
                    START MIGRATION TO {
                        module default {
                            type A6;
                            type Object6 {
                                required link a -> default::A6;
                            };
                        };
                    };
                    POPULATE MIGRATION;
                    COMMIT MIGRATION;
                    '''
                )

            await conn1.execute("COMMIT MIGRATION")

            await self.assert_query_in_conn(
                conn1,
                r"""
                    SELECT schema::ObjectType {
                        name,
                        links: {
                            target: {name}
                        }
                        FILTER .name = 'a'
                        ORDER BY .name
                    }
                    FILTER .name in {'default::Object5', 'default::Object6'};
                """,
                [
                    {
                        "name": "default::Object5",
                        "links": [
                            {
                                "target": {
                                    "name": "default::A5"
                                }
                            }
                        ]
                    }
                ],
            )

            await self.assert_query_in_conn(
                conn2,
                r"""
                    SELECT schema::ObjectType {
                        name,
                        links: {
                            target: {name}
                        }
                        FILTER .name = 'a'
                        ORDER BY .name
                    }
                    FILTER .name in {'default::Object5', 'default::Object6'};
                """,
                [
                    {
                        "name": "default::Object6",
                        "links": [
                            {
                                "target": {
                                    "name": "default::A6"
                                }
                            }
                        ]
                    }
                ],
            )

        finally:
            await conn1.aclose()
            await conn2.aclose()
            await self.con.execute('drop namespace temp1;')
            await self.con.execute('drop namespace temp2;')

    async def test_concurrent_schema_version_change_in_one_ns(self):
        await self.con.execute("create namespace temp1;")

        conn1 = await self.connect(database=self.get_database_name())
        conn2 = await self.connect(database=self.get_database_name())

        try:
            await conn1.execute('use namespace temp1;')
            await conn2.execute('use namespace temp1;')

            await conn1.execute(
                '''
                START MIGRATION TO {
                    module default {
                        type A5;
                        type Object5 {
                            required link a -> default::A5;
                        };
                    };
                };
                '''
            )
            await conn1.execute('POPULATE MIGRATION')
            async with conn2.transaction():
                await conn2.execute(
                    '''
                    START MIGRATION TO {
                        module default {
                            type A6;
                            type Object6 {
                                required link a -> default::A6;
                            };
                        };
                    };
                    POPULATE MIGRATION;
                    COMMIT MIGRATION;
                    '''
                )

            with self.assertRaises(edgedb.TransactionError):
                await conn1.execute("COMMIT MIGRATION")

        finally:
            await conn1.aclose()
            await conn2.aclose()
            await self.con.execute('drop namespace temp1;')
