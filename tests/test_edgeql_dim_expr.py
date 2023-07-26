#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


class TestDimExpr(tb.QueryTestCase):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'dim_expr.esdl'
    )

    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'dim_expr_setup.edgeql'
    )

    async def test_edgeql_tree_select_children(self):
        await self.assert_query_result(
            r"""
                SELECT cal::children(
                        (SELECT Tree
                        FILTER .name = '00')
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [{'name': '000', 'val': 'aaa', 'children': []}]
        )

    async def test_edgeql_tree_select_ichildren(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ichildren(
                        (SELECT Tree
                        FILTER .name = '00')
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '00', 'val': 'aa',
                 'children': [{'name': '000', 'val': 'aaa'}]},
                {'name': '000', 'val': 'aaa', 'children': []},
            ]
        )

    async def test_edgeql_tree_select_base(self):
        await self.assert_query_result(
            r"""
                SELECT cal::base(
                        (SELECT Tree
                        FILTER .name = '0')
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
            ]
        )

    async def test_edgeql_tree_select_ibase(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ibase(
                        (SELECT Tree
                        FILTER .name = '0')
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'a',
                 'children': [
                     {'name': '00', 'val': 'aa'},
                     {'name': '01', 'val': 'ab'},
                     {'name': '02', 'val': 'ac'},
                 ]},
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
            ]
        )

    async def test_edgeql_tree_select_descendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::descendant(
                        (SELECT Tree
                        FILTER .name = '0')
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '00', 'val': 'aa',
                 'children': [
                     {'name': '000', 'val': 'aaa'}
                 ]},
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '01', 'val': 'ab',
                 'children': [
                     {'name': '010', 'val': 'aba'}
                 ]},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
            ]
        )

    async def test_edgeql_tree_select_idescendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::idescendant(
                        (SELECT Tree
                        FILTER .name = '0')
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'a',
                 'children': [
                     {'name': '00', 'val': 'aa'},
                     {'name': '01', 'val': 'ab'},
                     {'name': '02', 'val': 'ac'},
                 ]},
                {'name': '00', 'val': 'aa',
                 'children': [
                     {'name': '000', 'val': 'aaa'}
                 ]},
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '01', 'val': 'ab',
                 'children': [
                     {'name': '010', 'val': 'aba'}
                 ]},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
            ]
        )

    async def test_edgeql_graph_select_children(self):
        await self.assert_query_result(
            r"""
                SELECT cal::children(
                        (SELECT Graph
                        FILTER .val = 'Dev'
                        LIMIT 1)
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [{'name': '0001', 'val': 'Alice', 'children': []},
             {'name': '0002', 'val': 'Bob', 'children': []},
             {'name': '0003', 'val': 'Cindy', 'children': []}
             ]
        )

    async def test_edgeql_graph_select_ichildren(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ichildren(
                        (SELECT Graph
                        FILTER .val = 'Dev'
                        LIMIT 1)
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '01', 'val': 'Dev',
                 'children': [
                     {'name': '0001', 'val': 'Alice'},
                     {'name': '0002', 'val': 'Bob'},
                     {'name': '0003', 'val': 'Cindy'}
                 ]},
            ]
        )

    async def test_edgeql_graph_select_base(self):
        await self.assert_query_result(
            r"""
                SELECT cal::base(
                        (SELECT Graph
                        FILTER .val = 'Duty'
                        LIMIT 1)
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
            ]
        )

    async def test_edgeql_graph_select_ibase(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ibase(
                        (SELECT Graph
                        FILTER .val = 'Duty'
                        LIMIT 1)
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'Duty',
                 'children': [
                     {'name': '01', 'val': 'Dev'},
                     {'name': '02', 'val': 'Test'},
                     {'name': '03', 'val': 'MainTain'}
                 ]},
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
            ]
        )

    async def test_edgeql_graph_select_descendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::descendant(
                        (SELECT Graph
                        FILTER .val = 'Project'
                        LIMIT 1)
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
                {'name': '11', 'val': 'Project1',
                 'children': [
                     {'name': '0001', 'val': 'Alice'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0005', 'val': 'Enne'}
                 ]},
                {'name': '12', 'val': 'Project2',
                 'children': [
                     {'name': '0002', 'val': 'Bob'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},
                {'name': '13', 'val': 'Project3',
                 'children': [
                     {'name': '0003', 'val': 'Cindy'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},

            ]
        )

    async def test_edgeql_graph_select_idescendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::idescendant(
                        (SELECT Graph
                        FILTER .val = 'Project'
                        LIMIT 1)
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
                {'name': '1', 'val': 'Project',
                 'children': [
                     {'name': '11', 'val': 'Project1'},
                     {'name': '12', 'val': 'Project2'},
                     {'name': '13', 'val': 'Project3'}
                 ]},
                {'name': '11', 'val': 'Project1',
                 'children': [
                     {'name': '0001', 'val': 'Alice'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0005', 'val': 'Enne'}
                 ]},
                {'name': '12', 'val': 'Project2',
                 'children': [
                     {'name': '0002', 'val': 'Bob'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},
                {'name': '13', 'val': 'Project3',
                 'children': [
                     {'name': '0003', 'val': 'Cindy'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},

            ]
        )

    async def test_edgeql_tree_select_root_children(self):
        await self.assert_query_result(
            r"""
                SELECT cal::children(
                        <Tree> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'a',
                 'children': [
                     {'name': '00', 'val': 'aa'},
                     {'name': '01', 'val': 'ab'},
                     {'name': '02', 'val': 'ac'},
                 ]},
                {'name': '1', 'val': 'b',
                 'children': [
                     {'name': '10', 'val': 'ba'},
                     {'name': '11', 'val': 'bb'},
                     {'name': '12', 'val': 'bc'},
                     {'name': '13', 'val': 'bd'},
                 ]},
            ]
        )

    async def test_edgeql_tree_select_root_ichildren(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ichildren(
                        <Tree> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'a',
                 'children': [
                     {'name': '00', 'val': 'aa'},
                     {'name': '01', 'val': 'ab'},
                     {'name': '02', 'val': 'ac'},
                 ]},
                {'name': '1', 'val': 'b',
                 'children': [
                     {'name': '10', 'val': 'ba'},
                     {'name': '11', 'val': 'bb'},
                     {'name': '12', 'val': 'bc'},
                     {'name': '13', 'val': 'bd'},
                 ]},
            ]
        )

    async def test_edgeql_tree_select_root_base(self):
        await self.assert_query_result(
            r"""
                SELECT cal::base(
                        <Tree> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
                {'name': '10', 'val': 'ba', 'children': []},
                {'name': '11', 'val': 'bb', 'children': []},
                {'name': '12', 'val': 'bc', 'children': []},
                {'name': '13', 'val': 'bd', 'children': []},
            ]
        )

    async def test_edgeql_tree_select_root_ibase(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ibase(
                        <Tree> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
                {'name': '10', 'val': 'ba', 'children': []},
                {'name': '11', 'val': 'bb', 'children': []},
                {'name': '12', 'val': 'bc', 'children': []},
                {'name': '13', 'val': 'bd', 'children': []},
            ]
        )

    async def test_edgeql_tree_select_root_descendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::descendant(
                        <Tree> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'a',
                 'children': [
                     {'name': '00', 'val': 'aa'},
                     {'name': '01', 'val': 'ab'},
                     {'name': '02', 'val': 'ac'},
                 ]},
                {'name': '00', 'val': 'aa',
                 'children': [
                     {'name': '000', 'val': 'aaa'}
                 ]},
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '01', 'val': 'ab',
                 'children': [
                     {'name': '010', 'val': 'aba'}
                 ]},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
                {'name': '1', 'val': 'b',
                 'children': [
                     {'name': '10', 'val': 'ba'},
                     {'name': '11', 'val': 'bb'},
                     {'name': '12', 'val': 'bc'},
                     {'name': '13', 'val': 'bd'},
                 ]},
                {'name': '10', 'val': 'ba', 'children': []},
                {'name': '11', 'val': 'bb', 'children': []},
                {'name': '12', 'val': 'bc', 'children': []},
                {'name': '13', 'val': 'bd', 'children': []},

            ]
        )

    async def test_edgeql_tree_select_root_idescendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::idescendant(
                        <Tree> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'a',
                 'children': [
                     {'name': '00', 'val': 'aa'},
                     {'name': '01', 'val': 'ab'},
                     {'name': '02', 'val': 'ac'},
                 ]},
                {'name': '00', 'val': 'aa',
                 'children': [
                     {'name': '000', 'val': 'aaa'}
                 ]},
                {'name': '000', 'val': 'aaa', 'children': []},
                {'name': '01', 'val': 'ab',
                 'children': [
                     {'name': '010', 'val': 'aba'}
                 ]},
                {'name': '010', 'val': 'aba', 'children': []},
                {'name': '02', 'val': 'ac', 'children': []},
                {'name': '1', 'val': 'b',
                 'children': [
                     {'name': '10', 'val': 'ba'},
                     {'name': '11', 'val': 'bb'},
                     {'name': '12', 'val': 'bc'},
                     {'name': '13', 'val': 'bd'},
                 ]},
                {'name': '10', 'val': 'ba', 'children': []},
                {'name': '11', 'val': 'bb', 'children': []},
                {'name': '12', 'val': 'bc', 'children': []},
                {'name': '13', 'val': 'bd', 'children': []},
            ]
        )

    async def test_edgeql_graph_select_root_children(self):
        await self.assert_query_result(
            r"""
                SELECT cal::children(
                        <Graph> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'Duty',
                 'children': [
                     {'name': '01', 'val': 'Dev'},
                     {'name': '02', 'val': 'Test'},
                     {'name': '03', 'val': 'MainTain'},
                 ]},
                {'name': '1', 'val': 'Project',
                 'children': [
                     {'name': '11', 'val': 'Project1'},
                     {'name': '12', 'val': 'Project2'},
                     {'name': '13', 'val': 'Project3'},
                 ]}
            ]
        )

    async def test_edgeql_graph_select_root_ichildren(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ichildren(
                        <Graph> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'Duty',
                 'children': [
                     {'name': '01', 'val': 'Dev'},
                     {'name': '02', 'val': 'Test'},
                     {'name': '03', 'val': 'MainTain'},
                 ]},
                {'name': '1', 'val': 'Project',
                 'children': [
                     {'name': '11', 'val': 'Project1'},
                     {'name': '12', 'val': 'Project2'},
                     {'name': '13', 'val': 'Project3'},
                 ]}
            ]
        )

    async def test_edgeql_graph_select_root_base(self):
        await self.assert_query_result(
            r"""
                SELECT cal::base(
                        <Graph> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
            ]
        )

    async def test_edgeql_graph_select_root_ibase(self):
        await self.assert_query_result(
            r"""
                SELECT cal::ibase(
                        <Graph> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
            ]
        )

    async def test_edgeql_graph_select_root_descendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::descendant(
                        <Graph> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'Duty',
                 'children': [
                     {'name': '01', 'val': 'Dev'},
                     {'name': '02', 'val': 'Test'},
                     {'name': '03', 'val': 'MainTain'},
                 ]},
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
                {'name': '01', 'val': 'Dev',
                 'children': [
                     {'name': '0001', 'val': 'Alice'},
                     {'name': '0002', 'val': 'Bob'},
                     {'name': '0003', 'val': 'Cindy'}
                 ]},
                {'name': '02', 'val': 'Test',
                 'children': [
                     {'name': '0004', 'val': 'Dannie'}
                 ]},
                {'name': '03', 'val': 'MainTain',
                 'children': [
                     {'name': '0005', 'val': 'Enne'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},
                {'name': '1', 'val': 'Project',
                 'children': [
                     {'name': '11', 'val': 'Project1'},
                     {'name': '12', 'val': 'Project2'},
                     {'name': '13', 'val': 'Project3'},
                 ]},
                {'name': '11', 'val': 'Project1',
                 'children': [
                     {'name': '0001', 'val': 'Alice'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0005', 'val': 'Enne'}
                 ]},
                {'name': '12', 'val': 'Project2',
                 'children': [
                     {'name': '0002', 'val': 'Bob'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},
                {'name': '13', 'val': 'Project3',
                 'children': [
                     {'name': '0003', 'val': 'Cindy'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]}
            ]
        )

    async def test_edgeql_graph_select_root_idescendant(self):
        await self.assert_query_result(
            r"""
                SELECT cal::idescendant(
                        <Graph> {}
                ) {
                    name,
                    val,
                    children: {
                        name,
                        val
                    } ORDER BY .name,
                }
                ORDER BY .name;
            """,
            [
                {'name': '0', 'val': 'Duty',
                 'children': [
                     {'name': '01', 'val': 'Dev'},
                     {'name': '02', 'val': 'Test'},
                     {'name': '03', 'val': 'MainTain'},
                 ]},
                {'name': '0001', 'val': 'Alice', 'children': []},
                {'name': '0002', 'val': 'Bob', 'children': []},
                {'name': '0003', 'val': 'Cindy', 'children': []},
                {'name': '0004', 'val': 'Dannie', 'children': []},
                {'name': '0005', 'val': 'Enne', 'children': []},
                {'name': '0006', 'val': 'Frank', 'children': []},
                {'name': '01', 'val': 'Dev',
                 'children': [
                     {'name': '0001', 'val': 'Alice'},
                     {'name': '0002', 'val': 'Bob'},
                     {'name': '0003', 'val': 'Cindy'}
                 ]},
                {'name': '02', 'val': 'Test',
                 'children': [
                     {'name': '0004', 'val': 'Dannie'}
                 ]},
                {'name': '03', 'val': 'MainTain',
                 'children': [
                     {'name': '0005', 'val': 'Enne'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},
                {'name': '1', 'val': 'Project',
                 'children': [
                     {'name': '11', 'val': 'Project1'},
                     {'name': '12', 'val': 'Project2'},
                     {'name': '13', 'val': 'Project3'},
                 ]},
                {'name': '11', 'val': 'Project1',
                 'children': [
                     {'name': '0001', 'val': 'Alice'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0005', 'val': 'Enne'}
                 ]},
                {'name': '12', 'val': 'Project2',
                 'children': [
                     {'name': '0002', 'val': 'Bob'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},
                {'name': '13', 'val': 'Project3',
                 'children': [
                     {'name': '0003', 'val': 'Cindy'},
                     {'name': '0004', 'val': 'Dannie'},
                     {'name': '0006', 'val': 'Frank'}
                 ]},

            ]
        )

    async def test_edgeql_tree_bounded_attr(self):
        await self.assert_query_result(
            r"""
                SELECT Tree {
                    name,
                    bases := array_agg(cal::base(
                        (select Tree filter .name = .name)
                    ).name)
                }
                ORDER BY .name;
            """,
            [
                {"name": "0", "bases": ["02", "000", "010"]},
                {"name": "00", "bases": ["000"]},
                {"name": "000", "bases": ["000"]},
                {"name": "01", "bases": ["010"]},
                {"name": "010", "bases": ["010"]},
                {"name": "02", "bases": ["02"]},
                {"name": "1", "bases": ["10", "11", "12", "13"]},
                {"name": "10", "bases": ["10"]},
                {"name": "11", "bases": ["11"]},
                {"name": "12", "bases": ["12"]},
                {"name": "13", "bases": ["13"]},
            ]
        )

    async def test_edgeql_graph_bounded_attr(self):
        await self.assert_query_result(
            r"""
                SELECT Graph {
                    name,
                    bases := array_agg(
                        (select cal::base((select Graph filter .name = .name))
                         order by .name).name
                    )
                }
                ORDER BY .name;
            """,
            [
                {"name": '0', "bases": ['0001', '0002', '0003', '0004', '0005', '0006']},
                {"name": '0001', "bases": ['0001']},
                {"name": '0002', "bases": ['0002']},
                {"name": '0003', "bases": ['0003']},
                {"name": '0004', "bases": ['0004']},
                {"name": '0005', "bases": ['0005']},
                {"name": '0006', "bases": ['0006']},
                {"name": '01', "bases": ['0001', '0002', '0003']},
                {"name": '02', "bases": ['0004']},
                {"name": '03', "bases": ['0005', '0006']},
                {"name": '1', "bases": ['0001', '0002', '0003', '0004', '0005', '0006']},
                {"name": '11', "bases": ['0001', '0004', '0005']},
                {"name": '12', "bases": ['0002', '0004', '0006']},
                {"name": '13', "bases": ['0003', '0004', '0006']},
            ]
        )


class TestDimExprSourceProp(TestDimExpr):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'dim_expr_left.esdl'
    )


class TestDimExprTargetProp(TestDimExpr):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'dim_expr_right.esdl'
    )


class TestDimExprBoth(TestDimExpr):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'dim_expr_both.esdl'
    )
