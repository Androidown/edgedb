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
import json
import os.path

import edgedb
from edb.testbase import server as tb


class TestEdgeQLLinkPathConstraint(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'linkpath_constraint.esdl')
    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'linkpath_constraint_setup.edgeql')

    async def fetch_result(self, edgeql: str):
        raw_res = await self.con._fetchall_json(edgeql)
        return json.loads(raw_res)

    async def test_alter_link_set_type_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"cannot .* because this affects 'target_property' of .*",
        ):
            await self.con.execute("""
                ALTER TYPE Trainer {
                    ALTER LINK fav_pet {
                        SET TYPE Animal using (SELECT Animal limit 1);
                    };
                }
            """)

    async def test_alter_link_set_type_02(self):
        ql = "SELECT Trainer {fav_pet: {species}} order by .fav_pet.species;"
        res = await self.fetch_result(ql)
        await self.con.execute("""
            ALTER TYPE Trainer {
                ALTER LINK fav_pet {
                    SET TYPE Animal using (SELECT Animal filter .species = Trainer.fav_pet.species);
                    on id to species;
                };
            }
        """)
        await self.assert_query_result(ql, res)

    async def test_alter_link_set_type_03(self):
        ql = "SELECT Trainer {pets: {species} order by .species} order by .name;"
        res = await self.fetch_result(ql)
        await self.con.execute("""
            ALTER TYPE Trainer {
                ALTER LINK pets {
                    SET TYPE Animal using (SELECT Animal filter .species = Trainer.pets.species);
                    on id to species;
                };
            }
        """)
        await self.assert_query_result(ql, res)

    async def test_alter_link_from_descendant_01(self):
        await self.con.execute("""
            CREATE TYPE Satoshi extending Trainer;
        """)

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r".* is prohibited, alter that on '.*' instead.",
        ):
            await self.con.execute("""
                ALTER TYPE Satoshi {
                    ALTER LINK fav_pet {
                        on name to species;
                    };
                }
            """)

    async def test_linkpath_no_exclusive_constraint_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"invalid link source property for .*, .* is not exclusive.",
        ):
            await self.con.execute("""
                ALTER TYPE Trainer {
                    ALTER LINK fav_pet {
                        on gender to species;
                    };
                }
            """)

    async def test_linkpath_no_exclusive_constraint_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"invalid link target property for .*, .* is not exclusive.",
        ):
            await self.con.execute("""
                ALTER TYPE Trainer {
                    ALTER LINK fav_pet {
                        on id to nickname;
                    };
                }
            """)

    async def test_linkpath_no_exclusive_constraint_03(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"invalid link source property for .*, .* is not exclusive.",
        ):
            await self.con.execute("""
                CREATE TYPE Haruka {
                    CREATE PROPERTY age -> int16;
                    CREATE LINK ace -> Pokemon {
                        on age to species;
                    };
                }
            """)

    async def test_linkpath_no_exclusive_constraint_04(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"invalid link target property for .*, .* is not exclusive.",
        ):
            await self.con.execute("""
                CREATE TYPE Haruka {
                    CREATE PROPERTY name -> str {
                        CREATE CONSTRAINT exclusive;
                    };
                    CREATE LINK ace -> Pokemon {
                        on name to nickname;
                    };
                }
            """)

