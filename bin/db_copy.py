#!/usr/bin/env python3
import logging
import asyncio
from datetime import datetime

from vzclient.asyncio import DatabaseCopy

logger = logging.getLogger("dbcopy.py")

DEFAULT_CONFIG = {
    "defaults": {
        "begin": None,
        "end": None,
        "max_gap": None,
        "measurement": "volkszaehler",
        "field_name": "value",
        "copy_tags": ["uuid", "unit", "type", "title"],
        "add_tags": {},
        "chunk_size": 8192,
        "transform": None,
        "buffer_size": 1000000,

        #
        "source": {
            "driver": "mysql",
            "host": "localhost",
            "user": "volkszaehler",
            "database": "volkszaehler",
            "secret": "",
        },
#
        "destination": {
            "driver": "influx",
            "host": "localhost",
            "bucket": "volkszaehler",
            "org": "volkszaehler",
            "secret": ""
        }
    },
    "include": ["*"],
    "exclude": {
        "titles": [],
        "types": ["virtual*"],
        "ids": [],
        "classes": []
    }
}


async def main():
    logging.basicConfig(level=logging.DEBUG)
    dbcopy = DatabaseCopy.from_yaml("config.yaml", DEFAULT_CONFIG)
    list_channels = False

    if list_channels:
        channels = await dbcopy.get_channels()
        for channel in channels:
            print(DatabaseCopy.get_name(channel), channel.get('type', "n.a"))
    else:
        await dbcopy.copy()

    start = datetime.utcnow()

    runtime = (datetime.utcnow() - start).total_seconds()
    logging.info("Total execution time: {}s".format(runtime))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    #loop.set_debug(True)
    loop.run_until_complete(main())
    loop.close()