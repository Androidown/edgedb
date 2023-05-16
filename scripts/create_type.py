from typing import Dict, List, NamedTuple
import requests


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
    to: str = None
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


def create_facility(link_to, link_card='single', has_link_prop=True):
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

    return CreateTypeBody(
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


def create_member_from_outer():
    return CreateTypeBody(
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


body = create_member_from_outer()

r = requests.post('http://127.0.0.1:5656/db/demo_dump/extern/create-type',
                  json=body.as_dict())
print(r.text)

body = create_facility('Member', 'multi')

r = requests.post('http://127.0.0.1:5656/db/demo_dump/extern/create-type',
                  json=body.as_dict())
print(r.text)
