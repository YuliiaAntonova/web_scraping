import csv
import re

import tqdm
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from itertools import chain

out = []
c = []
res = []
temp1 = []
temp2 = []
j = []
res_list = []

data_dict = []


async def get_soup(session, url):
    async with session.get(url=url) as resp:
        return BeautifulSoup(await resp.text(), "lxml")


async def worker(session, q):
    while True:
        url, link_name, title = await q.get()
        # area = title + ' ' + link_name
        # print(area)
        soup = await get_soup(session, url)
        a = soup.select('b')[1]
        for t in a:
            area = t.text
        # area = soup.select('font[size="2"]')
        # print(area)
        table = soup.select('tr[class="domino-viewentry"]')

        # d = {k: [] for k in keys}
        for post in table:

            d = post.select("[href*='display']")

            h = post.select("u")
            if len(h):
                for ii in h:
                    section = ii.text
                    # print(section)
                    term = ' '.join(section.split()[2:])

            if len(d):
                for jj in d:
                    IEV_ref = jj.text
                    m = 'https://www.electropedia.org/iev/iev.nsf/' + jj["href"]
                    out.append(m)

                    res.append({'link': m, 'Area': area, 'Section': section, 'Term': term, 'IEV ref': IEV_ref})

        q.task_done()


async def worker_link(session, q):
    while True:
        url = await q.get()

        soup = await get_soup(session, url)

        data = []
        table = soup.find('table', attrs={'class': 'table1'})
        # table_body = table.find('tbody')

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')

            cols = [ele.text.strip() for ele in cols if ele]

            data.append([ele for ele in cols if ele])

        for t in data:
            for j in t:
                if j.startswith('[SOURCE:'):
                    t.remove(j)
        result = list(filter(lambda x: x, data))

        d = {}

        i = 'en'
        for x in result[2:]:
            try:
                d[x[0]] = x[1]
            except IndexError:
                d['descr' + ' ' + str(i)] = x[0]
                i = 'fn'
        d.update({'link': url})
        res_list.append(d)

        q.task_done()


async def main():
    url = "https://www.electropedia.org/"

    async with aiohttp.ClientSession() as session:
        soup = await get_soup(session, url)

        titles = soup.select('td[width="28"]')

        links = soup.select("[href*='/iev/iev.nsf/']")

        committees = []
        for a, t in zip(links, titles):

            if a["href"].startswith('/iev/iev.nsf'):
                committees.append(
                    [
                        "https://www.electropedia.org/" + a["href"],
                        a.get_text(strip=True),
                        t.get_text(strip=True),
                    ]
                )

        queue = asyncio.Queue(maxsize=16)

        tasks = []

        # create 16 workers that will process data in parallel
        for i in range(16):
            task = asyncio.create_task(worker(session, queue))
            tasks.append(task)

        # put some data to worker queue
        for c in tqdm.tqdm(committees):
            await queue.put(c)

        # wait for all data to be processed
        await queue.join()

        # cancel all worker tasks
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

        # Phase 2 - Get next 18096 links:

        tasks = []

        # create 16 workers that will process data in parallel
        for i in range(16):
            task = asyncio.create_task(worker_link(session, queue))
            tasks.append(task)

        for c in tqdm.tqdm(out):
            await queue.put(c)

        # wait for all data to be processed
        await queue.join()

        # cancel all worker tasks
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

        cache = {}
        for dct in chain(res, res_list):
            b = dct["link"]
            cache.setdefault(b, dct).update(dct)

        temp_csv = list(cache.values())

        with open(f"elec3.csv", "w") as file:
            writer = csv.writer(file)
            writer.writerow(
                ("link",
                 "Area",
                 "Section",
                 "Term",
                 "IEV ref",
                 "en",
                 "descr en",
                 "fr",
                 "descr fn",
                 "ar",
                 "de",
                 "es",
                 "fi",
                 "it",
                 "ko",
                 "ja",
                 "nl BE",
                 "mn",
                 "pl",
                 "pt",
                 "ru",
                 "sr",
                 "sv",
                 "zh"
                 )
            )

        for isp in temp_csv:
            with open(f"elec3.csv", "a") as file:
                writer = csv.writer(file)
                writer.writerow(
                    (
                        isp.get("link", ''),
                        isp.get("Area", ''),
                        isp.get("Section", ''),
                        isp.get("Term", ''),
                        isp.get("IEV ref", ''),
                        isp.get("en", ''),
                        isp.get("descr en", ''),
                        isp.get("fr", ''),
                        isp.get("descr fn", ''),
                        isp.get("ar", ''),
                        isp.get("de", ''),
                        isp.get("es", ''),
                        isp.get("fi", ''),
                        isp.get("it", ''),
                        isp.get("ko", ''),
                        isp.get("ja", ''),
                        isp.get("nl BE", ''),
                        isp.get("mn", ''),
                        isp.get("pl", ''),
                        isp.get("pt", ''),
                        isp.get("ru", ''),
                        isp.get("sr", ''),
                        isp.get("sv", ''),
                        isp.get("zh", ''),
                    )
                )


if __name__ == "__main__":
    asyncio.run(main())
