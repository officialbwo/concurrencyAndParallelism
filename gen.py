#!/usr/bin/env python3
# coding=utf-8
#


import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import subprocess
from datetime import datetime, timedelta
import numpy


async def createFile(file, d, station):
    """create csv file"""
    for entry in range(ENTRYC):
        TT_RAND = numpy.random.uniform(TT_FLOOR, TT_CEILING)
        RF_RAND = numpy.random.uniform(RF_FLOOR, RF_CEILING)
        hourly = d + timedelta(hours=entry)
        MESS_DATUM = d.strftime('%Y-%m-%d %H:%M:%S')
        line = (str(station) + "," +
                str(MESS_DATUM) + ",3," +
                str(TT_RAND)[:5] + "," +
                str(RF_RAND)[:5] + "\n")
        file.write(line)
    file.close()

async def instance(thread):
    """create monitor tasks to write csv"""
    loop = asyncio.get_running_loop()
    stationsc_start = int((STATIONSC / THREADS) * thread)
    stationsc_stop = int(stationsc_start + STATIONSC / THREADSC)
    for station in range(stationsc_start, stationsc_stop):
        tasks = []
        for year in range(30):
            d = datetime.today() - timedelta(days=year*365)
            filename = ("stundenwerte_" +
                        str(station) + "_" +
                        str(d.strftime('%Y-%m-%d')) + ".csv")
            f = open(filename, "a")
            tasks.append(
                loop.create_task(
                    createFile(f,d, station)))
        asyncio.gather(*tasks)

async def main(executor):
    loop = asyncio.get_running_loop()
    teken = []
    for thread in range(THREADSC):
        teken.append(asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(
                instance(thread), loop
            )
        ))
    while not teken[len(teken) - 1].done() or len(teken) == 0:
        await asyncio.sleep(1)
        print("waitforit")

if __name__ == "__main__":
    THREADSC = 10
    TT_CEILING = 40
    TT_FLOOR = -10
    RF_CEILING = 100
    RF_FLOOR = 0

    STATIONSC=10000
    ENTRYC=24*365
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor(max_workers=THREADSC)
    try:
        loop.run_until_complete(main(executor))
    finally:
        loop.close()
