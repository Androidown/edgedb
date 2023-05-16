import asyncio
import json
import os

from asyncpg import connection as pg_connection
import edgedb


class AsyncBuffer:
    def __init__(self):
        self.datas = []

    async def append(self, value: bytes):
        self.datas.append(value)

    async def __aiter__(self):
        for value in self.datas:
            yield value


async def main(dbname, valid_query, expect_result, sql_for_external=None):
    # edgedb==0.21.0
    print(f"Is v0 protocol: {tuple([int(i) for i in edgedb.__version__.split('.')]) <= (0, 23, 0)}")
    client_from = edgedb.create_async_client(host='127.0.0.1', port=5656, database=dbname,
                                             tls_security='insecure')
    header = AsyncBuffer()
    body = AsyncBuffer()
    try:
        await client_from.ensure_connected()

        async with client_from._acquire() as conn:
            await conn._inner._impl._protocol.dump(header_callback=header.append, block_callback=body.append)

        print("Dump done.")

        if (
            (data := (await client_from.query_json(f"select sys::Database filter .name='restored_{dbname}'")))
            and len(json.loads(data)) > 0
        ):
            await client_from.execute(f"drop database restored_{dbname}")
        await client_from.execute(f"create database restored_{dbname}")
        if sql_for_external is not None:
            pg_conn = await pg_connection.connect(
                dsn=f"postgresql://postgres:@127.0.0.1:5432?database=V2f147ded60_restored_{dbname}"
            )
            await pg_conn.execute(sql_for_external)
            await pg_conn.close()

        print("To db prepared.")
        client_to = edgedb.create_async_client(host='127.0.0.1', port=5656, database=f'restored_{dbname}',
                                               tls_security='insecure')
        try:
            await client_to.ensure_connected()
            async with client_to._acquire() as conn:
                await conn._inner._impl._protocol.restore(header=b"".join(header.datas), data_gen=body)
            print("Restore done.")

            data = await client_to.query_json(valid_query)
            assert json.loads(data) == expect_result

            print('Data valid successfully.')

        finally:
            await client_to.aclose()
    finally:
        await client_from.aclose()


if __name__ == '__main__':
    # case no external
    asyncio.run(main(
        'cards',
        """
        SELECT User {
                name,
                deck: {
                    name,
                    element,
                    cost,
                    @count
                } ORDER BY @count DESC THEN .name ASC
            } ORDER BY .name
        """,
        [
            {
                'name': 'Alice',
                'deck': [
                    {
                        'cost': 2,
                        'name': 'Bog monster',
                        '@count': 3,
                        'element': 'Water'
                    },
                    {
                        'cost': 3,
                        'name': 'Giant turtle',
                        '@count': 3,
                        'element': 'Water'
                    },
                    {
                        'cost': 5,
                        'name': 'Dragon',
                        '@count': 2,
                        'element': 'Fire'
                    },
                    {
                        'cost': 1,
                        'name': 'Imp',
                        '@count': 2,
                        'element': 'Fire'
                    },
                ],
            },
            {
                'name': 'Bob',
                'deck': [
                    {
                        'cost': 2,
                        'name': 'Bog monster',
                        '@count': 3,
                        'element': 'Water'
                    },
                    {
                        'cost': 1,
                        'name': 'Dwarf',
                        '@count': 3,
                        'element': 'Earth'
                    },
                    {
                        'cost': 3,
                        'name': 'Giant turtle',
                        '@count': 3,
                        'element': 'Water'
                    },
                    {
                        'cost': 3,
                        'name': 'Golem',
                        '@count': 3,
                        'element': 'Earth'
                    },
                ],
            },
            {
                'name': 'Carol',
                'deck': [
                    {
                        'cost': 1,
                        'name': 'Dwarf',
                        '@count': 4,
                        'element': 'Earth'
                    },
                    {
                        'cost': 1,
                        'name': 'Sprite',
                        '@count': 4,
                        'element': 'Air'
                    },
                    {
                        'cost': 2,
                        'name': 'Bog monster',
                        '@count': 3,
                        'element': 'Water'
                    },
                    {
                        'cost': 2,
                        'name': 'Giant eagle',
                        '@count': 3,
                        'element': 'Air'
                    },
                    {
                        'cost': 3,
                        'name': 'Giant turtle',
                        '@count': 2,
                        'element': 'Water'
                    },
                    {
                        'cost': 3,
                        'name': 'Golem',
                        '@count': 2,
                        'element': 'Earth'
                    },
                    {
                        'cost': 4,
                        'name': 'Djinn',
                        '@count': 1,
                        'element': 'Air'
                    },
                ],
            },
            {
                'name': 'Dave',
                'deck': [
                    {
                        'cost': 1,
                        'name': 'Sprite',
                        '@count': 4,
                        'element': 'Air'
                    },
                    {
                        'cost': 2,
                        'name': 'Bog monster',
                        '@count': 1,
                        'element': 'Water'
                    },
                    {
                        'cost': 4,
                        'name': 'Djinn',
                        '@count': 1,
                        'element': 'Air'
                    },
                    {
                        'cost': 5,
                        'name': 'Dragon',
                        '@count': 1,
                        'element': 'Fire'
                    },
                    {
                        'cost': 2,
                        'name': 'Giant eagle',
                        '@count': 1,
                        'element': 'Air'
                    },
                    {
                        'cost': 3,
                        'name': 'Giant turtle',
                        '@count': 1,
                        'element': 'Water'
                    },
                    {
                        'cost': 3,
                        'name': 'Golem',
                        '@count': 1,
                        'element': 'Earth'
                    },
                ],
            }
        ]
    ))
    PROJECT_PATH = os.path.dirname(os.path.dirname(__file__))
    # case with external
    with open(os.path.join(PROJECT_PATH, 'tests', 'schemas', 'http_create_type.sql'), 'rt') as f:
        outter_schema = f.read()
    asyncio.run(main(
        'demo_dump',
        """
        SELECT distinct
            Facility {
                fid, name,
                bookedby: {
                    mid,
                    fullname := .surname ++ '.' ++ .firstname
                } ORDER BY .mid,
            } FILTER .fid=1;
        """,
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
        }],
        outter_schema
    ))
