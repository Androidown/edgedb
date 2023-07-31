import os
import re
from typing import Dict, List, NamedTuple

import edgedb

from edb.testbase import http as http_tb
from edb.testbase import server as server_tb


def as_dict(self=None) -> Dict:
    payload = {}
    for k, v in self._asdict().items():
        if v is None:
            continue
        if isinstance(v, list):
            payload[k] = [e.as_dict() for e in v]
        elif isinstance(v, (PropertyDetail, LinkDetail, Annotation)):
            payload[k] = v.as_dict()
        else:
            payload[k] = v
    return payload


class Annotation(NamedTuple):
    name: str
    value: str = None

    as_dict = as_dict


class PropertyDetail(NamedTuple):
    name: str
    type: str = None
    alias: str = None
    cardinality: str = None
    required: bool = None
    expr: str = None
    exclusive: bool = None
    annotations: List[Annotation] = None

    as_dict = as_dict


class LinkDetail(NamedTuple):
    name: str
    to: str = None
    type: str = None
    expr: str = None
    alias: str = None
    cardinality: str = None
    required: bool = None
    from_: str = None
    relation: str = None
    source: str = None
    target: str = None
    properties: List[PropertyDetail] = None
    annotations: List[Annotation] = None

    as_dict = as_dict


class CreateTypeBody(NamedTuple):
    module: str
    name: str
    relation: str
    properties: List[PropertyDetail] = None
    links: List[LinkDetail] = None
    annotations: List[Annotation] = None

    as_dict = as_dict


RE_CASE = re.compile(
    'test\_link\_(?P<from_>[a-z]*)\_(?P<to_>[a-z]*)(?:\_(?P<link_type>[a-z]*)\_link)?(?:\_(?P<with_prop>with\_prop))?'
)


class HttpCreateTypeMixin:
    async def link_outer_outer_single_link(self):
        # query object without link
        await self.assert_query_result(
            r'''
            SELECT
                Facility {
                    fid, discount, name
                } FILTER .fid = 1;
            ''',
            [{
                'fid': 1,
                'discount': 5 / 25,
                'name': 'Tennis Court 2'
            }]
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT distinct
                Facility {
                    fid, name,
                    booked_by: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                    comp_booked_by: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                } FILTER .fid=1;
            ''',
            [{
                'fid': 1,
                'name': 'Tennis Court 2',
                'booked_by': {'mid': 3, 'fullname': 'Rownam.Tim'},
                'comp_booked_by': {'mid': 1, 'fullname': 'Smith.Darren'},
            }]
        )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .fid = '1';
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .booked_by.mid = '1';
                '''
            )

    async def link_outer_outer_single_link_with_prop(self):
        # query object without link
        await self.assert_query_result(
            r'''
            SELECT
                Facility {
                    fid, discount, name
                } FILTER .fid = 1;
            ''',
            [{
                'fid': 1,
                'discount': 5 / 25,
                'name': 'Tennis Court 2'
            }]
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT Facility {
                    fid, name,
                    bookedby: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                } FILTER .fid=1;
            ''',
            [{
                'fid': 1,
                'name': 'Tennis Court 2',
                'bookedby': {'mid': 0, 'fullname': 'GUEST.GUEST'}
            }]
        )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .fid = '1';
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .bookedby.mid = '1';
                '''
            )

    async def link_outer_outer_multi_link_with_prop(self):
        # query object without link
        await self.assert_query_result(
            r'''
            SELECT
                Facility {
                    fid, discount, name
                } FILTER .fid = 1;
            ''',
            [{
                'fid': 1,
                'discount': 5 / 25,
                'name': 'Tennis Court 2'
            }]
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT distinct
                Facility {
                    fid, name,
                    bookedby: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                } FILTER .fid=1;
            ''',
            [{
                'fid': 1,
                'name': 'Tennis Court 2',
                'bookedby': [
                    {'mid': 0, 'fullname': 'GUEST.GUEST'},
                    {'mid': 1, 'fullname': 'Smith.Darren'},
                    {'mid': 2, 'fullname': 'Smith.Tracy'},
                    {'mid': 3, 'fullname': 'Rownam.Tim'},
                    {'mid': 4, 'fullname': 'Joplette.Janice'},
                    {'mid': 5, 'fullname': 'Butters.Gerald'},
                    {'mid': 6, 'fullname': 'Tracy.Burton'},
                    {'mid': 7, 'fullname': 'Dare.Nancy'},
                    {'mid': 8, 'fullname': 'Boothe.Tim'},
                    {'mid': 9, 'fullname': 'Stibbons.Ponder'},
                    {'mid': 10, 'fullname': 'Owen.Charles'},
                    {'mid': 11, 'fullname': 'Jones.David'},
                    {'mid': 12, 'fullname': 'Baker.Anne'},
                    {'mid': 13, 'fullname': 'Farrell.Jemima'},
                    {'mid': 14, 'fullname': 'Smith.Jack'},
                    {'mid': 15, 'fullname': 'Bader.Florence'},
                    {'mid': 16, 'fullname': 'Baker.Timothy'},
                    {'mid': 24, 'fullname': 'Sarwin.Ramnaresh'},
                    {'mid': 27, 'fullname': 'Rumney.Henrietta'},
                    {'mid': 28, 'fullname': 'Farrell.David'},
                    {'mid': 30, 'fullname': 'Purview.Millicent'},
                    {'mid': 35, 'fullname': 'Hunt.John'},
                ]
            }]
        )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .fid = '1';
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .bookedby.mid = '1';
                '''
            )

    async def link_outer_inner_single_link(self):
        link_to = "Person"
        # query object without link
        await self.assert_query_result(
            r'''
            SELECT
                Facility {
                    fid, discount, name
                } FILTER .fid = 1;
            ''',
            [{
                'fid': 1,
                'discount': 5 / 25,
                'name': 'Tennis Court 2'
            }]
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT distinct
                Facility {
                    fid, name,
                    booked_by: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                    comp_booked_by: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                } FILTER .fid=1;
            ''',
            [{
                'fid': 1,
                'name': 'Tennis Court 2',
                'booked_by': {'mid': 3, 'fullname': 'Rownam.Tim'},
                'comp_booked_by': {'mid': 1, 'fullname': 'Smith.Darren'}
            }]
        )
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                regex='deletion of .* is prohibited by link target policy'
        ):
            await self.con.execute(
                f'''
                delete {link_to} FILTER .mid=6;
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .fid = '1';
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .booked_by.mid = '1';
                '''
            )

    async def link_outer_inner_single_link_with_prop(self):
        link_to = "Person"
        # query object without link
        await self.assert_query_result(
            r'''
            SELECT
                Facility {
                    fid, discount, name
                } FILTER .fid = 1;
            ''',
            [{
                'fid': 1,
                'discount': 5 / 25,
                'name': 'Tennis Court 2'
            }]
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT Facility {
                    fid, name,
                    bookedby: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                } FILTER .fid=1;
            ''',
            [{
                'fid': 1,
                'name': 'Tennis Court 2',
                'bookedby': {'mid': 0, 'fullname': 'GUEST.GUEST'}
            }]
        )

        with self.assertRaisesRegex(
            edgedb.ConstraintViolationError,
            regex='deletion of .* is prohibited by link target policy'
        ):
            await self.con.execute(
                f'''
                delete {link_to} FILTER .mid=0;
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .fid = '1';
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .bookedby.mid = '1';
                '''
            )

    async def link_outer_inner_multi_link_with_prop(self):
        link_to = "Person"
        # query object without link
        await self.assert_query_result(
            r'''
            SELECT
                Facility {
                    fid, discount, name
                } FILTER .fid = 1;
            ''',
            [{
                'fid': 1,
                'discount': 5 / 25,
                'name': 'Tennis Court 2'
            }]
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT distinct
                Facility {
                    fid, name,
                    bookedby: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } ORDER BY .mid,
                } FILTER .fid=1;
            ''',
            [{
                'fid': 1,
                'name': 'Tennis Court 2',
                'bookedby': [
                    {'mid': 0, 'fullname': 'GUEST.GUEST'},
                    {'mid': 1, 'fullname': 'Smith.Darren'},
                    {'mid': 2, 'fullname': 'Smith.Tracy'},
                    {'mid': 3, 'fullname': 'Rownam.Tim'},
                    {'mid': 4, 'fullname': 'Joplette.Janice'},
                    {'mid': 5, 'fullname': 'Butters.Gerald'},
                    {'mid': 6, 'fullname': 'Tracy.Burton'},
                    {'mid': 7, 'fullname': 'Dare.Nancy'},
                    {'mid': 8, 'fullname': 'Boothe.Tim'},
                    {'mid': 9, 'fullname': 'Stibbons.Ponder'},
                    {'mid': 10, 'fullname': 'Owen.Charles'},
                    {'mid': 11, 'fullname': 'Jones.David'},
                    {'mid': 12, 'fullname': 'Baker.Anne'},
                    {'mid': 13, 'fullname': 'Farrell.Jemima'},
                    {'mid': 14, 'fullname': 'Smith.Jack'},
                    {'mid': 15, 'fullname': 'Bader.Florence'},
                    {'mid': 16, 'fullname': 'Baker.Timothy'},
                    {'mid': 24, 'fullname': 'Sarwin.Ramnaresh'},
                    {'mid': 27, 'fullname': 'Rumney.Henrietta'},
                    {'mid': 28, 'fullname': 'Farrell.David'},
                    {'mid': 30, 'fullname': 'Purview.Millicent'},
                    {'mid': 35, 'fullname': 'Hunt.John'},
                ]
            }]
        )

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                regex='deletion of .* is prohibited by link target policy'
        ):
            await self.con.execute(
                f'''
                delete {link_to} FILTER .mid=5;
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .fid = '1';
                '''
            )
        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select Facility FILTER .bookedby.mid = '1';
                '''
            )

    async def link_inner_outer(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                regex="target_property is required in "
                      "create link 'member' from object type 'default::NameList' "
                      "to external object type 'default::Member'."
        ):
            await self.con.execute(
                '''
                create type NameList{
                    create property _id -> std::int32 {
                        create constraint std::exclusive;
                    };
                    create link member -> Member;
                    create property alive -> std::bool;
                };
                '''
            )
        await self.con.execute(
            '''
            create type NameList{
                create property _id -> std::int32 {
                    create constraint std::exclusive;
                };
                create link member -> Member{
                    on _id to mid
                };
                create property alive -> std::bool;
            };
            '''
        )
        self.new_outter_type.append("NameList")
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                regex="target_property is required in "
                      "alter link 'member' from object type 'default::NameList' "
                      "to external object type 'default::Member'."
        ):
            await self.con.execute(
                '''
                alter type NameList{
                    alter link member {on id to id};
                };
                '''
            )
        await self.con.execute(
            '''
            insert NameList{
                _id := 0, alive := true,
                member:= (select Member filter .mid = 0)
            };
            insert NameList{
                _id := 1, alive := false,
                member:= (select Member filter .mid = 1)
            };
            insert NameList{
                _id := 2, alive := true,
                member:= (select Member filter .mid = 2)
            };
            '''
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT NameList
             {
                    member: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } Limit 1
             } FILTER .alive=true
             ORDER BY .member.mid;
            ''',
            [
                {'member': {'mid': 0, 'fullname': 'GUEST.GUEST'}},
                {'member': {'mid': 2, 'fullname': 'Smith.Tracy'}},
            ]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select NameList FILTER .member.mid = '1';
                '''
            )

    async def link_inner_outer_on_source_delete(self):
        await self.con.execute(
            '''
            create type NameList{
                create property _id -> std::int32 {
                    create constraint std::exclusive;
                };
                create link member -> Member {
                    on _id to mid;
                    on source delete delete target;
                };
                create property alive -> std::bool;
            };
            '''
        )
        await self.con.execute(
            '''
            insert NameList{
                _id := 0, alive := true,
                member:= (select Member filter .mid = 0)
            };
            insert NameList{
                _id := 1, alive := false,
                member:= (select Member filter .mid = 1)
            };
            insert NameList{
                _id := 2, alive := true,
                member:= (select Member filter .mid = 2)
            };
            '''
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT NameList
             {
                    member: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } Limit 1
             } FILTER .alive=true
             ORDER BY .member.mid;
            ''',
            [
                {
                    'member': {'mid': 0, 'fullname': 'GUEST.GUEST'}
                },
                {
                    'member': {'mid': 2, 'fullname': 'Smith.Tracy'}
                },
            ]
        )
        await self.assert_query_result(
            r'''
            SELECT count(Member);
            ''',
            [
                32
            ]
        )
        await self.con.execute(
            f'''
            delete NameList FILTER ._id=0;
            '''
        )
        await self.assert_query_result(
            r'''
            SELECT NameList
             {
                    member: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } Limit 1
             } FILTER .alive=true;
            ''',
            [
                {
                    'member': {'mid': 2, 'fullname': 'Smith.Tracy'}
                },
            ]
        )
        await self.assert_query_result(
            r'''
            SELECT count(Member);
            ''',
            [
                32
            ]
        )
        self.new_outter_type.append("NameList")

    async def link_inner_outer_multi_link_on_source_delete(self):
        await self.con.execute(
            '''
            create type NameList{
                create property _id -> std::int32 {
                    create constraint std::exclusive;
                };
                create multi link member -> Member {
                    on _id to mid;
                    on source delete delete target;
                };
                create property alive -> std::bool;
            };
            '''
        )
        await self.con.execute(
            '''
            insert NameList{
                _id := 0, alive := true,
                member:= (select Member filter .mid = 0)
            };
            insert NameList{
                _id := 1, alive := false,
                member:= (select Member filter .mid = 1)
            };
            insert NameList{
                _id := 2, alive := true,
                member:= (select Member filter .mid = 2)
            };
            '''
        )
        # query object with link
        await self.assert_query_result(
            r'''
            SELECT NameList
             {
                    member: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } Limit 1
             } FILTER .alive=true;
            ''',
            [
                {
                    'member': [{'mid': 0, 'fullname': 'GUEST.GUEST'}]
                },
                {
                    'member': [{'mid': 2, 'fullname': 'Smith.Tracy'}]
                },
            ]
        )
        await self.assert_query_result(
            r'''
            SELECT count(Member);
            ''',
            [
                32
            ]
        )
        await self.con.execute(
            f'''
            delete NameList FILTER ._id=0;
            '''
        )
        await self.assert_query_result(
            r'''
            SELECT NameList
             {
                    member: {
                        mid,
                        fullname := .surname ++ '.' ++ .firstname
                    } Limit 1
             } FILTER .alive=true;
            ''',
            [
                {
                    'member': [{'mid': 2, 'fullname': 'Smith.Tracy'}]
                },
            ]
        )
        await self.assert_query_result(
            r'''
            SELECT count(Member);
            ''',
            [
                32
            ]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidTypeError,
                regex="operator '=' cannot be applied to operands of type .*"
        ):
            await self.con.execute(
                r'''
                select NameList FILTER .member.mid = '1';
                '''
            )

        self.new_outter_type.append("NameList")

    async def dml_reject(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                regex='External .* is read-only.'
        ):
            await self.con.execute(
                '''
                delete Member filter .mid = 0;
                '''
            )
        with self.assertRaisesRegex(
                edgedb.QueryError,
                regex='External .* is read-only.'
        ):
            await self.con.execute(
                '''
                update Member filter .mid = 0
                set {mid := 999};
                '''
            )
        with self.assertRaisesRegex(
                edgedb.QueryError,
                regex='External .* is read-only.'
        ):
            await self.con.execute(
                '''
                insert Member
                {
                   mid := 999, surname := 'T', firstname := 'E',
                   address := 'Space', phone_number := '31415926',
                   joindate := <cal::local_datetime>'1970-01-01 00:00:00'
                };
                '''
            )

    async def link_outer_inner_multi_link_with_prop_check_annotations(self):
        await self.assert_query_result(
            r'''
            with module schema
            select ObjectType {
                name,
                links: {
                    name,
                    properties: { name, annotations: {name, value := @value} } 
                                filter .name not in {'source', 'target'}
                                order by .name,
                } filter .name != '__type__',
                properties: {
                    name,
                    annotations: {name, value := @value}
                } filter .name != 'id'
                  order by .name,
                annotations: {name, value := @value} order by .name,
                external
            }
            filter .name = 'default::Facility'
            ''',
            [
                {
                    'name': 'default::Facility',
                    'links': [
                        {
                            'name': 'bookedby',
                            'properties': [
                                {'name': 'slots', 'annotations': []},
                                {'name': 'start_at',
                                 'annotations': [{'name': 'std::description', 'value': 'start time for booking'}]},
                            ]
                        }
                    ],
                    'properties': [
                        {
                            'name': 'discount',
                            'annotations': []
                        },
                        {
                            'name': 'fid',
                            'annotations': [
                                {'name': 'std::description', 'value': 'facid from Facility'}
                            ]
                        },
                        {
                            'name': 'guest_cost',
                            'annotations': [
                                {'name': 'std::description', 'value': 'guestcost from Facility'}
                            ]
                        },
                        {
                            'name': 'member_cost',
                            'annotations': [
                                {'name': 'std::description', 'value': 'membercost from Facility'}
                            ]
                        },
                        {
                            'name': 'name',
                            'annotations': [
                                {'name': 'std::description', 'value': 'name from Facility'}
                            ]
                        },
                    ],
                    'annotations': [
                        {'name': 'std::description', 'value': 'Facility type from outter'},
                        {'name': 'std::title', 'value': 'Facility(external)'},
                    ],
                    'external': True
                }
            ]
        )


class TestHttpCreateType(http_tb.ExternTestCase, HttpCreateTypeMixin):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'http_create_type.esdl'
    )
    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'http_create_type_setup.edgeql'
    )

    # EdgeQL/HTTP queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    new_outter_type = []

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loop.run_until_complete(cls.prepare_external_db())

    @classmethod
    async def prepare_external_db(cls, dbname: str = None):
        outter_sql_path = os.path.join(
            os.path.dirname(__file__), 'schemas',
            'http_create_type.sql'
        )
        if os.path.exists(outter_sql_path):
            with open(outter_sql_path, 'rt') as f:
                outter_schema = f.read()
        else:
            raise OSError(f'Sql file with path : {outter_sql_path} for outter schema not found.')
        conn = await cls.pg_conn(dbname)
        try:
            await conn.sql_execute(outter_schema.encode())
        finally:
            conn.terminate()

    def setUp(self):
        super().setUp()
        self.create_relation_with_external()

    def create_relation_with_external(self):
        self.new_outter_type = []
        case_name = self._testMethodName
        if match := RE_CASE.match(case_name):
            from_, to_, link_type, link_prop = match.groups()
            if to_ == 'outer':
                self.assertTrue(self.create_member_from_outer())
                self.new_outter_type.append('Member')
                link_to = "Member"
            else:
                link_to = "Person"

            if link_prop == 'with_prop':
                has_link_prop = True
            else:
                has_link_prop = False

            if from_ == 'outer':
                self.assertTrue(self.create_facility(link_to, link_type, has_link_prop))
                self.new_outter_type.append('Facility')
        else:
            self.assertTrue(self.create_member_from_outer())
            self.new_outter_type.append('Member')

    def tearDown(self):
        try:
            while len(self.new_outter_type):
                t = self.new_outter_type.pop()
                self.loop.run_until_complete(self.con.execute(f'Drop type {t};'))
        finally:
            super().tearDown()

    def create_facility(self, link_to, link_card='single', has_link_prop=True):
        if has_link_prop or link_card == 'multi':
            if link_card == 'single':
                relation = "(select distinct on (facid) * from cd.bookings) as bookings"
            else:
                relation = "(select distinct on (facid, memid) * from cd.bookings) as bookings"
            link_detail = [
                LinkDetail(
                    name="bookedby",
                    cardinality=link_card,
                    relation=relation,
                    type=link_to,
                    source="facid",
                    target="memid",
                    from_="fid",
                    to="mid",
                    properties=[
                        PropertyDetail(
                            name="starttime",
                            type="timestamp",
                            alias="start_at",
                            annotations=[
                                Annotation(name='description', value='start time for booking')
                            ]
                        ),
                        PropertyDetail(
                            name="slots",
                            type="int4"
                        )
                    ] if has_link_prop else None,
                    annotations=[
                        Annotation(name='description', value='booked by link with link prop')
                    ]
                ),
            ]
        else:
            link_detail = [
                LinkDetail(
                    name="booked_by",
                    type=link_to,
                    to="mid",
                    annotations=[
                        Annotation(name='description', value='booked by link')
                    ]
                ),
                LinkDetail(
                    name="comp_booked_by",
                    expr=f"SELECT {link_to} filter .mid = __source__.fid",
                    annotations=[
                        Annotation(name='description', value='booked by computable link')
                    ]
                )
            ]

        return self.create_type(
            CreateTypeBody(
                module="default",
                name="Facility",
                relation="cd.facilities",
                properties=[
                    PropertyDetail(
                        name="facid",
                        type="int4",
                        alias="fid",
                        exclusive=True,
                        annotations=[
                            Annotation(name='description', value='facid from Facility')
                        ]
                    ),
                    PropertyDetail(
                        name="membercost",
                        type="numeric",
                        alias="member_cost",
                        annotations=[
                            Annotation(name='description', value='membercost from Facility')
                        ]
                    ),
                    PropertyDetail(
                        name="name",
                        type="str",
                        annotations=[
                            Annotation(name='description', value='name from Facility')
                        ]
                    ),
                    PropertyDetail(
                        name="guestcost",
                        type="numeric",
                        alias="guest_cost",
                        annotations=[
                            Annotation(name='description', value='guestcost from Facility')
                        ]
                    ),
                    PropertyDetail(
                        name="discount",
                        expr=".member_cost / .guest_cost",
                        annotations=[
                            Annotation(name='description', value='discount from Facility')
                        ]
                    )
                ],
                links=link_detail,
                annotations=[
                    Annotation(name='description', value='Facility type from outter'),
                    Annotation(name='title', value='Facility(external)')
                ]
            )
        )

    def create_member_from_outer(self):
        return self.create_type(
            CreateTypeBody(
                module="default",
                name="Member",
                relation="cd.members",
                properties=[
                    PropertyDetail(
                        name="memid",
                        type="int4",
                        alias="mid",
                        exclusive=True,
                    ),
                    PropertyDetail(
                        name="surname",
                        type="str",
                    ),
                    PropertyDetail(
                        name="firstname",
                        type="str"
                    ),
                    PropertyDetail(
                        name="address",
                        type="str",
                    ),
                    PropertyDetail(
                        name="zipcode",
                        type="int4"
                    ),
                    PropertyDetail(
                        name="telephone",
                        type="str",
                        alias="phone_number"
                    ),
                    PropertyDetail(
                        name="recommendedby",
                        type="int4"
                    ),
                    PropertyDetail(
                        name="joindate",
                        type="timestamp"
                    )
                ],
            )
        )

    async def test_link_outer_outer_single_link(self):
        await self.link_outer_outer_single_link()

    async def test_link_outer_outer_single_link_with_prop(self):
        await self.link_outer_outer_single_link_with_prop()

    async def test_link_outer_outer_multi_link_with_prop(self):
        await self.link_outer_outer_multi_link_with_prop()

    async def test_link_outer_inner_single_link(self):
        await self.link_outer_inner_single_link()

    async def test_link_outer_inner_single_link_with_prop(self):
        await self.link_outer_inner_single_link_with_prop()

    async def test_link_outer_inner_multi_link_with_prop(self):
        await self.link_outer_inner_multi_link_with_prop()

    async def test_link_inner_outer(self):
        await self.link_inner_outer()

    async def test_link_inner_outer_on_source_delete(self):
        await self.link_inner_outer_on_source_delete()

    async def test_link_inner_outer_multi_link_on_source_delete(self):
        await self.link_inner_outer_multi_link_on_source_delete()

    async def test_link_outer_inner_multi_link_with_prop_check_annotations(self):
        await self.link_outer_inner_multi_link_with_prop_check_annotations()

    async def test_dml_reject(self):
        await self.dml_reject()


class TestHttpCreateTypeDumpRestore(TestHttpCreateType, server_tb.StableDumpTestCase):
    async def prepare(self):
        await self.prepare_external_db(dbname=f"{self.get_database_name()}_restored")

    async def test_link_outer_outer_single_link(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_outer_outer_single_link,
            restore_db_prepare=self.prepare
        )

    async def test_link_outer_outer_single_link_with_prop(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_outer_outer_single_link_with_prop,
            restore_db_prepare=self.prepare
        )

    async def test_link_outer_outer_multi_link_with_prop(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_outer_outer_multi_link_with_prop,
            restore_db_prepare=self.prepare
        )

    async def test_link_outer_inner_single_link(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_outer_inner_single_link,
            restore_db_prepare=self.prepare
        )

    async def test_link_outer_inner_single_link_with_prop(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_outer_inner_single_link_with_prop,
            restore_db_prepare=self.prepare
        )

    async def test_link_outer_inner_multi_link_with_prop(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_outer_inner_multi_link_with_prop,
            restore_db_prepare=self.prepare
        )

    async def test_link_inner_outer(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_inner_outer,
            restore_db_prepare=self.prepare
        )
        self.new_outter_type.remove("NameList")

    async def test_link_inner_outer_on_source_delete(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_inner_outer_on_source_delete,
            restore_db_prepare=self.prepare
        )
        self.new_outter_type.remove("NameList")

    async def test_link_inner_outer_multi_link_on_source_delete(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_inner_outer_multi_link_on_source_delete,
            restore_db_prepare=self.prepare
        )
        self.new_outter_type.remove("NameList")

    async def test_link_outer_inner_multi_link_with_prop_check_annotations(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.link_outer_inner_multi_link_with_prop_check_annotations,
            restore_db_prepare=self.prepare
        )

    async def test_dml_reject(self):
        await self.check_dump_restore(
            check_method=HttpCreateTypeMixin.dml_reject,
            restore_db_prepare=self.prepare
        )