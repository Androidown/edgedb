import os
from typing import Dict, List, NamedTuple
import edgedb
from edb.testbase import http as tb


def as_dict(self=None) -> Dict:
    payload = {}
    for k, v in self._asdict().items():
        if v is None:
            continue
        if isinstance(v, list):
            payload[k] = [e.as_dict() for e in v]
        elif isinstance(v, (PropertyDetail, LinkDetail,)):
            payload[k] = v.as_dict()
        else:
            payload[k] = v
    return payload


class PropertyDetail(NamedTuple):
    name: str
    type: str = None
    alias: str = None
    cardinality: str = None
    required: bool = None
    expr: str = None
    exclusive: bool = None

    as_dict = as_dict


class LinkDetail(NamedTuple):
    name: str
    to: str
    type: str = None
    alias: str = None
    cardinality: str = None
    required: bool = None
    from_: str = None
    relation: str = None
    source: str = None
    target: str = None
    properties: List[PropertyDetail] = None

    as_dict = as_dict


class CreateTypeBody(NamedTuple):
    module: str
    name: str
    relation: str
    properties: List[PropertyDetail] = None
    links: List[LinkDetail] = None

    as_dict = as_dict


class TestHttpCreateType(tb.ExternTestCase):
    SCHEMA = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'http_create_type.esdl'
    )
    SETUP = os.path.join(
        os.path.dirname(__file__), 'schemas',
        'http_create_type_setup.esdl'
    )

    # EdgeQL/HTTP queries cannot run in a transaction
    TRANSACTION_ISOLATION = False

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        outter_sql_path = os.path.join(
            os.path.dirname(__file__), 'schemas',
            'http_create_type.sql'
        )
        if os.path.exists(outter_sql_path):
            with open(outter_sql_path, 'rt') as f:
                outter_schema = f.read()
        else:
            raise OSError(f'Sql file with path : {outter_sql_path} for outter schema not found.')
        conn = cls.loop.run_until_complete(cls.pg_conn())
        try:
            cls.loop.run_until_complete(conn.sql_execute(outter_schema.encode()))
        finally:
            conn.terminate()

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
                                alias="start_at"
                            ),
                            PropertyDetail(
                                name="slots",
                                type="int4"
                            )
                        ] if has_link_prop else None,
                    ),
                ]
        else:
            link_detail = [
                LinkDetail(
                    name="booked_by",
                    type=link_to,
                    from_="fid",
                    to="mid",
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
                    ),
                    PropertyDetail(
                        name="membercost",
                        type="numeric",
                        alias="member_cost"
                    ),
                    PropertyDetail(
                        name="name",
                        type="str"
                    ),
                    PropertyDetail(
                        name="guestcost",
                        type="numeric",
                        alias="guest_cost"
                    ),
                    PropertyDetail(
                        name="discount",
                        expr=".member_cost / .guest_cost"
                    )
                ],
                links=link_detail
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
        self.assertTrue(self.create_member_from_outer())
        link_to = "Member"
        self.assertTrue(self.create_facility(link_to, has_link_prop=False))
        try:
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
                    } FILTER .fid=1;
                ''',
                [{
                    'fid': 1,
                    'name': 'Tennis Court 2',
                    'booked_by': {'mid': 3, 'fullname': 'Rownam.Tim'}
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

        finally:
            await self.con.execute(
                '''
                Drop type Facility;
                Drop type Member;
                '''
            )

    async def test_link_outer_outer_single_link_with_prop(self):
        self.assertTrue(self.create_member_from_outer())
        link_to = "Member"
        self.assertTrue(self.create_facility(link_to))
        try:
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
        finally:
            await self.con.execute(
                '''
                Drop type Facility;
                Drop type Member;
                '''
            )

    async def test_link_outer_outer_multi_link_with_prop(self):
        self.assertTrue(self.create_member_from_outer())
        link_to = "Member"
        self.assertTrue(self.create_facility(link_to, 'multi'))
        try:
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
        finally:
            await self.con.execute(
                '''
                Drop type Facility;
                Drop type Member;
                '''
            )

    async def test_link_outer_inner_single_link(self):
        link_to = "Person"
        self.assertTrue(self.create_facility(link_to, has_link_prop=False))
        try:
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
                    } FILTER .fid=1;
                ''',
                [{
                    'fid': 1,
                    'name': 'Tennis Court 2',
                    'booked_by': {'mid': 3, 'fullname': 'Rownam.Tim'}
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
        finally:
            await self.con.execute(
                '''
                Drop type Facility;
                '''
            )

    async def test_link_outer_inner_single_link_with_prop(self):
        link_to = "Person"
        self.assertTrue(self.create_facility(link_to))
        try:
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
        finally:
            await self.con.execute(
                '''
                Drop type Facility;
                '''
            )

    async def test_link_outer_inner_multi_link_with_prop(self):
        link_to = "Person"
        self.assertTrue(self.create_facility(link_to, 'multi'))
        try:
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
        finally:
            await self.con.execute(
                '''
                Drop type Facility;
                '''
            )

    async def test_link_inner_outer(self):
        self.assertTrue(self.create_member_from_outer())
        try:
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
        finally:
            await self.con.execute(
                '''
                Drop type NameList;
                Drop type Member;
                '''
            )

    async def test_link_inner_outer_on_source_delete(self):
        self.assertTrue(self.create_member_from_outer())
        try:
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
        finally:
            await self.con.execute(
                '''
                Drop type NameList;
                Drop type Member;
                '''
            )

    async def test_link_inner_outer_multi_link_on_source_delete(self):
        self.assertTrue(self.create_member_from_outer())
        try:
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
        finally:
            await self.con.execute(
                '''
                Drop type NameList;
                Drop type Member;
                '''
            )

    async def test_dml_reject(self):
        self.assertTrue(self.create_member_from_outer())
        try:
            res = await self.con._fetchall_json(
                "describe module default;"
            )
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
        finally:
            await self.con.execute(
                '''
                Drop type Member;
                '''
            )
