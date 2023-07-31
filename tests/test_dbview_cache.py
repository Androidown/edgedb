import json
import os
import re
import unittest
from contextlib import contextmanager
from unittest import skipIf
from unittest.mock import Mock, call

import immutables
from edb.server.dbview.dbview import EqlDict, RankedDiskCache
from edb.server import defines
from edb.testbase import server as tb
from edb.testbase.server import _call_system_api

RE_COMPILER = re.compile(r"edgedb_server_edgeql_query_compilations_total{path=\"compiler\"} ([0-9]+\.0)?")
RE_PICKLE_DUMP = re.compile(r"edgedb_server_edgeql_cache_pickle_dump_duration_seconds_count ([0-9]+\.0)?")


def get_compiler_count(raw_metrics):
    for line in raw_metrics.split('\n'):
        if 'edgedb_server_edgeql_query_compilations_total' in line:
            if compiler_match := RE_COMPILER.match(line):
                return float(compiler_match.group(1))

    return 0.0


def get_dump_count(raw_metrics):
    for line in raw_metrics.split('\n'):
        if 'edgedb_server_edgeql_cache_pickle_dump_duration_seconds_count' in line:
            if match := RE_PICKLE_DUMP.match(line):
                return float(match.group(1))

    return 0.0


class TestDbviewCache(tb.QueryTestCase):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'cards.esdl'
    )

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'cards_setup.edgeql'
    )
    TRANSACTION_ISOLATION = False
    missed = None

    def tearDown(self):
        try:
            self.clear_cache()
        finally:
            super().tearDown()

    def clear_cache(self):
        conargs = self.cluster.get_connect_args()
        host, port = conargs['host'], conargs['port']
        _call_system_api(host, port, f'/view-cache/{self.get_database_name()}', False)

    @contextmanager
    def _check_cache(self, is_missed):
        compiler_count = get_compiler_count(self.fetch_metrics())
        yield
        missed = (get_compiler_count(self.fetch_metrics()) - compiler_count > 0)
        self.assertEqual(missed, is_missed)

    def assert_cache_missed(self):
        return self._check_cache(True)

    def assert_cache_hit(self):
        return self._check_cache(False)

    async def test_dbview_get_cache_common_01(self):
        # query first time, should compile
        await self.con._fetchall_json('select User.deck;')

        # query second time, should read cache
        with self.assert_cache_hit():
            await self.con._fetchall_json('select User.deck;')

    async def test_dbview_partly_clear_cache_01(self):
        await self.con._fetchall_json('select Bot;')
        await self.con._fetchall_json('select SpecialCard;')

        # DDL affecting SpecialCard
        await self.con.execute(
            '''
                ALTER TYPE Card {
                    create INDEX ON (__subject__.name);
                };
            '''
        )

        # cache for SpecialCard should be cleared, so should compile
        with self.assert_cache_missed():
            await self.con._fetchall_json('select SpecialCard;')

        # cache for Bot remained, so should read from cache
        with self.assert_cache_hit():
            await self.con._fetchall_json('select Bot;')

        await self.con.execute(
            '''
                ALTER TYPE Card {
                    drop INDEX ON (__subject__.name);
                };
            '''
        )

    async def test_dbview_partly_clear_cache_02(self):
        await self.con._fetchall_json('select Bot.deck;')
        await self.con._fetchall_json('select Card;')

        # DDL affecting Named
        await self.con.execute(
            '''
                ALTER TYPE Named {
                    create INDEX ON (__subject__.name);
                };
            '''
        )

        # cache for Card should be cleared, so should compile
        with self.assert_cache_missed():
            await self.con._fetchall_json('select Card;')

        # cache for Bot&Card should be cleared, so should compile
        with self.assert_cache_missed():
            await self.con._fetchall_json('select Bot.deck;')

        await self.con.execute(
            '''
                ALTER TYPE Named {
                    drop INDEX ON (__subject__.name);
                };
            '''
        )

    @skipIf(
        condition=(defines.SQL_BYTES_LENGTH_DISK_CACHE > 9000),
        reason='Test Query Bytes is 9203 length.'
    )
    async def test_db_cause_disk_cache_01(self):
        await self.con._fetchall_json(
            """
                    WITH
                        MODULE schema,
                        DCardT := (SELECT ObjectType
                                   FILTER .name = 'default::DCard'),
                        DCardOwners := (SELECT DCardT.links
                                        FILTER .name = 'owners')
                    SELECT
                        DCardOwners {
                            target[IS ObjectType]: {
                                name,
                                pointers: {
                                    name
                                } FILTER .name = 'name_upper'
                            }
                        }
                    """
        )
        self.assertGreater(get_dump_count(self.fetch_metrics()), 0)

    async def test_computable_property(self):
        await self.con._fetchall_json("SELECT User {deck_cost}")

        # -----------------------------------------------------------------------------
        # alter computable here
        await self.con.execute(
            '''
                ALTER TYPE User {
                    ALTER property deck_cost using (count(.deck));
                };
            '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT User {deck_cost}")

        with self.assert_cache_hit():
            await self.con._fetchall_json("SELECT User {deck_cost}")

        await self.con.execute(
            '''
                ALTER TYPE User {
                    ALTER property deck_cost using (sum(.deck.cost));
                };
            '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT User {deck_cost}")

    async def test_chained_computable_property(self):
        # -----------------------------------------------------------------------------
        # create chained computable here
        await self.con.execute(
            '''
                ALTER TYPE User {
                    CREATE property deck_cost_alias := .deck_cost;
                    CREATE property deck_cost_alias2 := .deck_cost_alias;
                };
            '''
        )

        await self.con._fetchall_json("SELECT User {deck_cost_alias2}")

        # -----------------------------------------------------------------------------
        # alter computable here
        await self.con.execute(
            '''
                ALTER TYPE User {
                    ALTER property deck_cost using (count(.deck));
                };
            '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT User {deck_cost_alias2}")

        await self.con.execute(
            '''
                ALTER TYPE User {
                    ALTER property deck_cost_alias using (.deck_cost + 1);
                };
            '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT User {deck_cost_alias2}")

        await self.con.execute(
            '''
                ALTER TYPE User {
                    DROP property deck_cost_alias2;
                    DROP property deck_cost_alias;
                    ALTER property deck_cost using (sum(.deck.cost));
                };
            '''
        )

    async def test_computable_single_link_outbound(self):
        await self.con._fetchall_json("SELECT Card {best_award}")

        # -----------------------------------------------------------------------------
        # alter computable here
        await self.con.execute(
            '''
                ALTER TYPE Card {
                    ALTER link best_award using
                    (select .awards limit 1);
                };
            '''
        )

        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {best_award}")

        await self.con.execute(
            '''
                        ALTER TYPE Card {
                            ALTER link best_award using
                            (select .awards order by .name limit 1);
                        };
                    '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {best_award}")

    async def test_computable_multi_link_outbound(self):
        await self.con._fetchall_json("SELECT Card {good_awards}")

        # -----------------------------------------------------------------------------
        # alter computable here
        await self.con.execute(
            '''
                ALTER TYPE Card {
                    ALTER link good_awards using
                    (SELECT .awards FILTER .name != '1st');
                };
            '''
        )

        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {good_awards}")

        await self.con.execute(
            '''
                ALTER TYPE Card {
                    ALTER link good_awards using
                    (SELECT .awards FILTER .name != '3rd');
                };
            '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {good_awards}")

    async def test_computable_link_inbound(self):
        await self.con._fetchall_json("SELECT Card {owners}")

        # -----------------------------------------------------------------------------
        # alter computable here
        await self.con.execute(
            '''
                ALTER TYPE Card {
                    ALTER link owners using
                    (.<deck);
                };
            '''
        )

        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {owners}")

        await self.con.execute(
            '''
                ALTER TYPE Card {
                    ALTER link owners using
                    (.<deck[IS User]);
                };
            '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {owners}")

    async def test_execute_script(self):
        await self.con.execute(
            "CREATE TYPE CacheInv_01 {CREATE REQUIRED PROPERTY prop1 -> std::str;};"
            "INSERT CacheInv_01 {prop1 := 'aaa'};"
        )

        res = await self.con._fetchall_json("SELECT CacheInv_01.prop1")

        self.assertEqual(json.loads(res), ['aaa'])

        await self.con.execute("DELETE (SELECT CacheInv_01);"
                               "ALTER TYPE CacheInv_01 {DROP PROPERTY prop1;};"
                               "ALTER TYPE CacheInv_01 {CREATE REQUIRED PROPERTY prop1 -> std::int64;};"
                               "INSERT CacheInv_01 {prop1 := 123};")

        with self.assert_cache_missed():
            res = await self.con._fetchall_json("SELECT CacheInv_01.prop1")

        self.assertEqual(json.loads(res), [123])

    async def test_alter_policies_invalid(self):
        await self.con._fetchall_json("SELECT Card {owners}")
        await self.con.execute(
            '''
            alter type Card {
                create access policy blind allow select using (false);
            };
            '''
        )
        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {owners}")

        with self.assert_cache_hit():
            await self.con._fetchall_json("SELECT Card {owners}")

        await self.con.execute(
            '''
            alter type Card {
                alter access policy blind using (true);
            };
            '''
        )

        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {owners}")

        with self.assert_cache_hit():
            await self.con._fetchall_json("SELECT Card {owners}")

        await self.con.execute(
            '''
            alter type Card {
                drop access policy blind;
            };
            '''
        )

        with self.assert_cache_missed():
            await self.con._fetchall_json("SELECT Card {owners}")

        with self.assert_cache_hit():
            await self.con._fetchall_json("SELECT Card {owners}")


session_config1 = immutables.Map({'name': 'k', 'value': 'o'})
alias1 = immutables.Map({'A': 'a', 'B': 'b'})

session_config2 = immutables.Map({'name': 'g', 'value': 'f'})
alias2 = immutables.Map({'C': 'c', 'D': 'd'})

session_config3 = immutables.Map({'name': 'm', 'value': 'j'})
alias3 = immutables.Map({'E': 'e', 'F': 'f'})

session_config4 = immutables.Map({'name': 'e', 'value': 't'})
alias4 = immutables.Map({'G': 'g', 'H': 'h'})

key1 = (session_config1, alias1)
key2 = (session_config2, alias2)
key3 = (session_config3, alias3)
key4 = (session_config4, alias4)


class TestEqlDict(unittest.TestCase):
    def setUp(self):
        self.obj_id_to_eql = EqlDict()

    def tearDown(self):
        self.obj_id_to_eql.clear()

    def test_assign_and_delete(self):
        self.obj_id_to_eql['aa-bb'] = {key1}
        self.obj_id_to_eql['cc-dd'] = {key1}
        self.obj_id_to_eql['ee-ff'] = {key2}

        self.assertEqual(self.obj_id_to_eql['aa-bb'], {key1})
        self.assertEqual(self.obj_id_to_eql['cc-dd'], {key1})
        self.assertEqual(self.obj_id_to_eql['ee-ff'], {key2})
        self.assertEqual(self.obj_id_to_eql.refering[key1], {'aa-bb', 'cc-dd'})
        self.assertEqual(self.obj_id_to_eql.refering[key2], {'ee-ff'})

        del self.obj_id_to_eql['aa-bb']
        self.assertNotIn('aa-bb', self.obj_id_to_eql)
        self.assertNotIn('cc-dd', self.obj_id_to_eql)
        self.assertNotIn(key1, self.obj_id_to_eql.refering)

        del self.obj_id_to_eql['ee-ff']
        self.assertNotIn('ee-ff', self.obj_id_to_eql)
        self.assertNotIn(key2, self.obj_id_to_eql.refering)

    def test_common_add_and_drop_with_eqls(self):
        self.obj_id_to_eql['aa-bb'] = {key1}
        self.obj_id_to_eql['cc-dd'] = {key1}
        self.obj_id_to_eql.add('aa-bb', key2)

        self.assertEqual(self.obj_id_to_eql['aa-bb'], {key1, key2})
        self.assertEqual(self.obj_id_to_eql['cc-dd'], {key1})
        self.assertEqual(self.obj_id_to_eql.refering[key1], {'aa-bb', 'cc-dd'})
        self.assertEqual(self.obj_id_to_eql.refering[key2], {'aa-bb'})

        self.obj_id_to_eql.maybe_drop_with_eqls({key2})
        self.assertEqual(self.obj_id_to_eql['aa-bb'], {key1})
        self.assertEqual(self.obj_id_to_eql.refering[key1], {'aa-bb', 'cc-dd'})
        self.assertNotIn(key2, self.obj_id_to_eql.refering)

    def test_drop_with_eqls_complex(self):
        self.obj_id_to_eql['aa-bb'] = {key1, key2}
        self.obj_id_to_eql['cc-dd'] = {key2, key3}
        self.obj_id_to_eql['ee-ff'] = {key3, key1}
        self.obj_id_to_eql['gg-hh'] = {key4}
        self.obj_id_to_eql.maybe_drop_with_eqls({key2, key3})

        self.assertEqual(self.obj_id_to_eql['aa-bb'], {key1})
        self.assertNotIn('cc-dd', self.obj_id_to_eql)
        self.assertEqual(self.obj_id_to_eql['ee-ff'], {key1})
        self.assertEqual(self.obj_id_to_eql['gg-hh'], {key4})

        self.assertEqual(self.obj_id_to_eql.refering[key1], {'aa-bb', 'ee-ff'})
        self.assertNotIn(key2, self.obj_id_to_eql.refering)
        self.assertNotIn(key3, self.obj_id_to_eql.refering)
        self.assertEqual(self.obj_id_to_eql.refering[key4], {'gg-hh'})

    def test_delete_complex(self):
        self.obj_id_to_eql['aa-bb'] = {key2}
        self.obj_id_to_eql['cc-dd'] = {key2, key3}
        self.obj_id_to_eql['ee-ff'] = {key3, key1}
        self.obj_id_to_eql['gg-hh'] = {key4}

        del self.obj_id_to_eql['cc-dd']
        self.assertNotIn('aa-bb', self.obj_id_to_eql)
        self.assertNotIn('cc-dd', self.obj_id_to_eql)
        self.assertEqual(self.obj_id_to_eql['ee-ff'], {key1})
        self.assertEqual(self.obj_id_to_eql['gg-hh'], {key4})

        self.assertEqual(self.obj_id_to_eql.refering[key1], {'ee-ff'})
        self.assertNotIn(key2, self.obj_id_to_eql.refering)
        self.assertNotIn(key3, self.obj_id_to_eql.refering)
        self.assertEqual(self.obj_id_to_eql.refering[key4], {'gg-hh'})

    def test_clear(self):
        self.obj_id_to_eql['aa-bb'] = {key1, key2}
        self.obj_id_to_eql['cc-dd'] = {key2, key3}
        self.obj_id_to_eql['ee-ff'] = {key3, key1}
        self.obj_id_to_eql['gg-hh'] = {key4}

        self.obj_id_to_eql.clear()
        self.assertEqual(len(self.obj_id_to_eql), 0)
        self.assertEqual(len(self.obj_id_to_eql.refering), 0)


bak_count = defines.DISK_CACHE_MAX_COUNT


class TestRankedDiskCache(unittest.TestCase):
    def setUp(self):
        defines.DISK_CACHE_MAX_COUNT = 3
        self.disk_cache = RankedDiskCache()
        self.cb = Mock()

    def tearDown(self):
        self.disk_cache.clear()
        defines.DISK_CACHE_MAX_COUNT = bak_count

    def test_clear(self):
        self.disk_cache[key1] = '/a'
        self.disk_cache[key2] = '/b'
        self.disk_cache.clear()
        self.assertEqual(len(self.disk_cache), 0)

    def test_clean_overload(self):
        self.disk_cache[key1] = '/a'
        self.disk_cache[key2] = '/b'
        self.disk_cache[key3] = '/c'
        self.disk_cache[key4] = '/d'
        self.assertEqual(len(self.disk_cache), 3)
        self.assertNotIn(key1, self.disk_cache)

    def test_set_with_cb(self):
        self.disk_cache.set_with_cb(key1, '/a', self.cb)
        self.disk_cache.set_with_cb(key2, '/b', self.cb)
        self.disk_cache.set_with_cb(key3, '/c', self.cb)
        self.disk_cache.set_with_cb(key4, '/d', self.cb)
        self.cb.assert_has_calls([call({key1})])

    def test_delete_with_cb(self):
        self.disk_cache[key1] = '/a'
        self.disk_cache[key2] = '/b'
        self.disk_cache.delete_with_cb(key2, self.cb)
        self.cb.assert_has_calls([call({key2})])
        self.assertNotIn(key2, self.disk_cache)

    def test_rank_1(self):
        self.disk_cache[key1] = '/a'
        self.disk_cache[key2] = '/b'
        self.disk_cache[key1]
        self.assertEqual(self.disk_cache.data.popitem(last=False)[0], key2)

    def test_rank_2(self):
        self.disk_cache[key1] = '/a'
        self.disk_cache[key2] = '/b'
        self.disk_cache[key1]
        self.disk_cache[key2]
        self.assertEqual(self.disk_cache.data.popitem(last=False)[0], key1)