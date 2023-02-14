from edb.testbase import server as tb


class TestEdgeQLComputablesDDL(tb.DDLTestCase):
    SETUP = """
        START MIGRATION TO {
            module default {
                type User {
                    required property name -> str {
                        constraint exclusive;
                        readonly := True;
                        default := 'Tony';
                        constraint max_len_value(10);
                    };
                    property nickname := .name;
                    property gender -> str;
                    property sex -> str;
                    property point -> int32 {
                        constraint max_value(100);
                    };
                    link obj -> Obj;
                    required property code -> int32 {
                        constraint exclusive
                    };
                    required property identifier -> int32;
                };
                type Obj;
            };
        };
        POPULATE MIGRATION;
        COMMIT MIGRATION;
    """

    async def assert_property_has_contraints(self, prop, n_cons):
        eql = f"""
             select count(
                schema::Constraint
                filter .subject.name='{prop}'
             );
        """
        await self.assert_query_result(eql, [n_cons])

    async def test_alter_alias_computable_keep_base(self):
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname {
                    using ((select User filter ( not exists .obj)).name)
                };
            };
        """)
        await self.assert_property_has_contraints('nickname', 2)

    async def test_alter_alias_computable_change_to_diff_alias(self):
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (.code)
            };
        """)
        await self.assert_property_has_contraints('nickname', 1)
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (.name)
            };
        """)
        await self.assert_property_has_contraints('nickname', 2)

    async def test_alter_alias_computable_change_to_diff_alias_dup_constraint(self):
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (.point)
            };
        """)
        await self.assert_property_has_contraints('nickname', 1)
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (.gender)
            };
        """)
        await self.assert_property_has_contraints('nickname', 0)

    async def test_alter_alias_computable_change_type(self):
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (len(.name))
            };
        """)
        await self.assert_property_has_contraints('nickname', 0)

    async def test_alter_alias_computable_change_cardinality(self):
        # -----------------------------------------------------------------------------
        # change from required to optional
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (
                    SELECT User.name filter User.gender = 'male'
                )
            };
        """)

    async def test_alter_alias_computable_change_optionality(self):
        # -----------------------------------------------------------------------------
        # change from required to optional
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (
                    .name ++ .gender
                )
            };
        """)
    async def test_alter_normal_to_alias_computable(self):
        # -----------------------------------------------------------------------------
        # change a normal property to computable-alias
        await self.con.execute("""
            ALTER type User {
                ALTER property identifier using (.code)
            };
        """)
        await self.assert_property_has_contraints('identifier', 1)

    async def test_drop_alias_computable(self):
        # -----------------------------------------------------------------------------
        # change from required to optional
        await self.con.execute("""
            ALTER type User {
                drop property nickname;
            };
        """)

    async def test_drop_alias_computable_with_dep(self):
        # -----------------------------------------------------------------------------
        # change from required to optional
        await self.con.execute("""
            ALTER type User {
                drop property nickname;
                drop property name;
            };
        """)

    async def test_drop_type_with_alias_computable(self):
        await self.con.execute("""
            DROP TYPE User;
        """)

