import asyncio
import json
import os
import pickle

import edgedb
from loguru import logger

PROJECT_PATH = os.path.dirname(os.path.dirname(__file__))
DUMP_DIR = os.path.join(PROJECT_PATH, 'dumps')
EDB_HOST = '127.0.0.1'
EDB_PORT = 5656


class AsyncBuffer:
    def __init__(self):
        self.datas = []

    async def append(self, value: bytes):
        self.datas.append(value)

    async def __aiter__(self):
        for value in self.datas:
            yield value


async def dump(dbname, modules, new_dbname):
    client_from = edgedb.create_async_client(
        host=EDB_HOST, port=EDB_PORT, database=dbname, timeout=300,
        tls_security='insecure'
    )
    with open('/edgedb/edgedb/dump_modules.txt', 'w') as fp:
        fp.write(str(modules))

    header = AsyncBuffer()
    body = AsyncBuffer()

    try:
        await client_from.ensure_connected()
        async with client_from._acquire() as conn:
            await conn._inner._impl._protocol.dump(header_callback=header.append, block_callback=body.append)

        logger.info("Dump done.")

    finally:
        await client_from.aclose()

    with open(os.path.join(DUMP_DIR, f"{new_dbname}.dump"), 'wb') as fp:
        pickle.dump((header, body), fp, protocol=pickle.HIGHEST_PROTOCOL)


async def prepare_target_db(name):
    sys_client = edgedb.create_async_client(
        host=EDB_HOST, port=EDB_PORT, database='edgedb', timeout=300,
        tls_security='insecure'
    )
    try:
        await sys_client.ensure_connected()
        if (
                (data := (await sys_client.query_json(f"select sys::Database filter .name='{name}'")))
                and len(json.loads(data)) > 0
        ):
            await sys_client.execute(f"drop database {name}")
        await sys_client.execute(f"create database {name}")
    finally:
        await sys_client.aclose()


async def restore(dbname):
    with open(os.path.join(DUMP_DIR, f"{dbname}.dump"), 'rb') as fp:
        header, body = pickle.loads(fp.read())

    client_to = edgedb.create_async_client(
        host=EDB_HOST, port=EDB_PORT, database=dbname, timeout=300,
        tls_security='insecure'
    )
    try:
        await client_to.ensure_connected()

        async with client_to._acquire() as conn:
            await conn._inner._impl._protocol.restore(header=b"".join(header.datas), data_gen=body)

        logger.info("Restore done.")
    finally:
        await client_to.aclose()


async def run_in_tx(client, ql):
    async for tx in client.transaction():
        async with tx:
            await tx.execute(ql)


async def module_rename(dbname):
    client = edgedb.create_async_client(
        host=EDB_HOST, port=EDB_PORT, database=dbname, timeout=300,
        tls_security='insecure'
    )
    try:
        await client.ensure_connected()
        new_mod_prepare = "\n".join(
            [
                f"Create module {new_mod} if not exists;"
                for new_mod in rename_map.values()
            ]
        )
        logger.info(f'Execute:\n{new_mod_prepare}')
        await run_in_tx(client, new_mod_prepare)

        objs = await client.query(
            " select schema::ObjectType{name} "
            "filter .external = False and not contains(.name, '(') and .module_name in {" +
            ",".join([f"'{old}'" for old in rename_map.keys()]) +
            "};"
        )
        global_objs = await client.query(
            " select schema::Global{name} "
            "filter .module_name in {" +
            ",".join([f"'{old}'" for old in rename_map.keys()]) +
            "};"
        )
        global_objs = [g.name for g in global_objs]
        for o in objs:
            is_global = False
            if o.name in global_objs:
                is_global = True
            old_mod, _, obj_name = o.name.rpartition("::")
            ql = "alter {type} {old_mod}::{obj_name} rename to {new_mod}::{obj_name};".format(
                old_mod=old_mod,
                new_mod=rename_map[old_mod],
                obj_name=obj_name,
                type='global' if is_global else 'type'
            )
            logger.info(f"Execute:\n" + ql)
            await run_in_tx(client, ql)

        mod_drop = "\n".join(
            [
                f"drop module {old_mod};"
                for old_mod in rename_map.keys()
            ]
        )
        logger.info(f'Execute:\n{mod_drop}')
        await run_in_tx(client, mod_drop)

    finally:
        await client.aclose()


rename_map = {
    'spaceulqtqb': 'bulqtqb',
    'appulqtqb003': 'ulqtqb003', 'appulqtqb006': 'ulqtqb006', 'appulqtqb007': 'ulqtqb007', 'appulqtqb008': 'ulqtqb008',
    'appulqtqb010': 'ulqtqb010', 'appulqtqb011': 'ulqtqb011', 'appulqtqb015': 'ulqtqb015', 'appulqtqb016': 'ulqtqb016',
}

# splitted db name -> modules
to_db_map = {
    'restored_spaceulqtqb': {
        'default', 'spaceulqtqb',
        'appulqtqb003', 'appulqtqb006', 'appulqtqb007', 'appulqtqb008',
        'appulqtqb010', 'appulqtqb011', 'appulqtqb015', 'appulqtqb016',
    },
}


async def main():
    from_db = 'alpha_db'
    for new_dbname, modules in to_db_map.items():
        logger.info(f'Dump data from {modules} in {from_db} to {new_dbname}...')
        # await dump(from_db, modules, new_dbname)
        # await prepare_target_db(new_dbname)
        # await restore(new_dbname)
        await module_rename(new_dbname)


if __name__ == '__main__':
    asyncio.run(main())
