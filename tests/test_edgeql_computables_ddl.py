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
                };
            };
        };
        POPULATE MIGRATION;
        COMMIT MIGRATION;
    """
    async def test_alter_alias_computable_change_type(self):
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (len(.name))
            };
        """)

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

    async def test_alter_alias_computable_add_dep(self):
        # -----------------------------------------------------------------------------
        # change from required to optional
        await self.con.execute("""
            ALTER type User {
                ALTER property nickname using (
                    .name ++ .gender
                )
            };
        """)

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

