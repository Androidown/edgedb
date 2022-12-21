import inspect
import pathlib
from edb.testbase import server as tb
from . import test_link_target_delete


class _PathedLinkTargetDelete(tb.QueryTestCase, borrows=test_link_target_delete.TestLinkTargetDeleteDeclarative):
    SCHEMA = pathlib.Path(__file__).parent / 'schemas' / 'link_tgt_del.esdl'
    TRANSACTION_ISOLATION = False

    _cycle_test = test_link_target_delete.TestLinkTargetDeleteDeclarative._cycle_test

    async def assert_all_borrowed_test(self):
        for methname in dir(self):
            if (
                methname.startswith('raw_')
                and inspect.iscoroutinefunction(meth := getattr(self, methname))
            ):
                await meth()


class TestInlineLinkTarget(_PathedLinkTargetDelete):
    async def test_alter_inl_link_target(self):
        await self.con.execute("""
        ALTER TYPE Source1 {
            ALTER LINK tgt1_restrict {
                on id to name;
            };
            ALTER LINK tgt1_allow {
                on id to name;
            };
            ALTER LINK tgt1_del_source {
                on id to name;
            };
            ALTER LINK tgt1_deferred_restrict {
                on id to name;
            };
            ALTER LINK self_del_target_orphan {
                on id to name;
            };
            ALTER LINK tgt1_del_target {
                on id to name;
            };
            ALTER LINK self_del_source {
                on id to name;
            };
            ALTER LINK tgt1_del_target_orphan {
                on id to name;
            };
        }
        """)
        await self.assert_all_borrowed_test()


class TestOutlineLinkTarget(_PathedLinkTargetDelete):
    async def test_alter_otl_link_target(self):
        await self.con.execute("""
        ALTER TYPE Source1 {
            ALTER LINK tgt1_m2m_allow {
                on id to name;
            };
            ALTER LINK tgt1_m2m_del_source {
                on id to name;
            };
            ALTER LINK tgt1_m2m_del_target {
                on id to name;
            };
            ALTER LINK tgt1_m2m_del_target_orphan {
                on id to name;
            };
            ALTER LINK tgt1_m2m_restrict {
                on id to name;
            };
        }
        """)
        await self.assert_all_borrowed_test()

class TestOutlineLinkSource(_PathedLinkTargetDelete):
    async def test_alter_otl_link_source(self):
        await self.con.execute("""
        ALTER TYPE Source1 {
            ALTER LINK tgt1_m2m_allow {
                on name to id;
            };
            ALTER LINK tgt1_m2m_del_source {
                on name to id;
            };
            ALTER LINK tgt1_m2m_del_target {
                on name to id;
            };
            ALTER LINK tgt1_m2m_del_target_orphan {
                on name to id;
            };
            ALTER LINK tgt1_m2m_restrict {
                on name to id;
            };
        }
        """)
        await self.assert_all_borrowed_test()


class TestOutlineLinkBoth(_PathedLinkTargetDelete):
    async def test_alter_otl_link_both(self):
        await self.con.execute("""
        ALTER TYPE Source1 {
            ALTER LINK tgt1_m2m_allow {
                on name to name;
            };
            ALTER LINK tgt1_m2m_del_source {
                on name to name;
            };
            ALTER LINK tgt1_m2m_del_target {
                on name to name;
            };
            ALTER LINK tgt1_m2m_del_target_orphan {
                on name to name;
            };
            ALTER LINK tgt1_m2m_restrict {
                on name to name;
            };
        }
        """)
        await self.assert_all_borrowed_test()
