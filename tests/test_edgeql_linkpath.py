#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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


import os.path
from edb.testbase import server as tb
from .test_edgeql_select import TestEdgeQLSelect


class TestEdgeQLLinkPath(TestEdgeQLSelect):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'linkpath.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.edgeql')
    async def test_edgeql_select_computable_03(self):
        await self.assert_query_result(
            r'''
            SELECT
                User {
                    name,
                    shortest_own_text := (
                        SELECT
                            Text {
                                body
                            }
                        FILTER
                            Text[IS Issue].owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT 1
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Initial public release of EdgeDB.',
                },
            }]
        )

    async def test_edgeql_select_computable_05(self):
        await self.assert_query_result(
            r'''
            WITH
                # we aren't referencing User in any way, so this works
                # best as a subquery, than inline computable
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                User {
                    name,
                    shortest_own_text := (
                        SELECT
                            Text {body}
                        FILTER
                            Text[IS Issue].owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT
                            1
                    ),
                    shortest_text := sub {
                        body
                    },
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Initial public release of EdgeDB.',
                },
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        )

    async def test_edgeql_select_computable_07(self):
        await self.assert_query_result(
            r'''
            SELECT
                User {
                    name,
                    # ad-hoc computable with many results
                    special_texts := (
                        SELECT Text {body}
                        FILTER Text[IS Issue].owner != User
                        ORDER BY len(Text.body) DESC
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'special_texts': [
                    {'body': 'We need to be able to render data in '
                             'tabular format.'},
                    {'body': 'Minor lexer tweaks.'}
                ],
            }]
        )



class TestEdgeQLLinkPathDDL(tb.QueryTestCase, borrows=TestEdgeQLSelect):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'linkpath.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.edgeql')

    async def _run_Issue_owner_link_cases(self):
        await self.raw_edgeql_select_unique_02()
        await self.raw_edgeql_select_computable_08()
        await self.raw_edgeql_select_computable_28()

    async def test_edgeql_linkpath_alter_single_link_src_prop_01(self):
        await self.con.execute("""
            ALTER TYPE Issue {
                ALTER LINK owner {
                    on number to id
                }
            };
        """)
        await self._run_Issue_owner_link_cases()

    async def test_edgeql_linkpath_alter_single_link_tgt_prop_01(self):
        await self.con.execute("""
            ALTER TYPE Issue {
                ALTER LINK owner {
                    on id to name
                }
            };
        """)
        await self._run_Issue_owner_link_cases()

    async def test_edgeql_linkpath_alter_single_link_both_01(self):
        await self.con.execute("""
            ALTER TYPE Issue {
                ALTER LINK owner {
                    on number to name
                }
            };
        """)
        await self._run_Issue_owner_link_cases()

    async def _run_Issue_watcher_link_cases(self):
        await self.raw_edgeql_select_nested_redefined_link()
        await self.raw_edgeql_select_cross05()
        await self.raw_edgeql_select_cross06()
        await self.raw_edgeql_select_cross_07()
        await self.raw_edgeql_select_cross08()
        await self.raw_edgeql_select_cross_09()
        await self.raw_edgeql_select_cross_10()
        await self.raw_edgeql_select_cross_11()
        await self.raw_edgeql_select_cross_12()
        await self.raw_edgeql_select_cross_13()
        await self.raw_edgeql_select_subqueries_08()
        await self.raw_edgeql_partial_02()
        await self.raw_edgeql_partial_03()

    async def test_edgeql_linkpath_alter_multi_link_src_prop_01(self):
        await self.con.execute("""
            ALTER TYPE Issue {
                ALTER LINK watchers {
                    on number to name
                }
            };
        """)
        await self._run_Issue_watcher_link_cases()

    async def test_edgeql_linkpath_alter_multi_link_tgt_prop_01(self):
        await self.con.execute("""
            ALTER TYPE Issue {
                ALTER LINK watchers {
                    on id to id
                }
            };
        """)
        await self._run_Issue_watcher_link_cases()

    async def test_edgeql_linkpath_alter_multi_link_both_01(self):
        self.subTest()
        await self.con.execute("""
            ALTER TYPE Issue {
                ALTER LINK watchers {
                    on number to id
                }
            };
        """)
        await self._run_Issue_watcher_link_cases()
