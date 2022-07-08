import csv
import re
from itertools import chain

import tqdm
import asyncio
import aiohttp
from bs4 import BeautifulSoup

out = []
res = []
res1 = []
links = []
data1 = []
iso_data = []
data_dict = []
status = []
out1 = []
summary_list = []


async def get_soup(session, url):
    async with session.get(url=url) as resp:
        return BeautifulSoup(await resp.content.read(), "lxml")


async def worker(session, q):
    while True:
        url, list_l, title = await q.get()
        soup = await get_soup(session, url)

        id_subsection = soup.select('td[width="5%"]')

        Subsection = soup.select('td[width="95%"]')

        for t, p in zip(id_subsection, Subsection):
            a1 = t.find('a', {'href': re.compile(r'./recommendation.asp')})

            out.append([
                "https://www.itu.int/rec" + a1["href"][1:],
                list_l,
                title,
                t.get_text(strip=True),
                p.text
            ])

        q.task_done()


#
async def worker_iso(session, q):
    while True:
        url, list_l, title, id_subsection, Subsection = await q.get()

        soup = await get_soup(session, url)

        data = []
        table = soup.find('table', attrs={'cellspacing': "1"})
        if table:

            rows = table.find_all('tr')

            for row in rows:
                cols = row.find_all('td')
                cols2 = row.select('td[nowrap]')
                col3 = row.select('td[nowrap] a')

                for item in cols2:
                    if item in cols:
                        cols.remove(item)

                col = [ele.text.strip() for ele in cols]

                data.append([ele for ele in col if ele])
                col2 = [ele.text.strip() for ele in cols2]
                status.append([ele for ele in col2 if ele])

            result1 = list(filter(lambda x: x, data[1:]))
            res = sum(result1, [])
            ll2 = [sublist for sublist in res if sublist != 'Superseded and Withdrawn components']

            resu = list(filter(lambda x: x, status[1:]))
            ll = [sublist for sublist in resu if sublist != ['Number', 'Title', 'Status']]

            for count, i in enumerate(ll):
                if len(i) > 0:
                    del i[0]
            status_list = sum(ll, [])

            link = table.select('a')

            for i, j, l in zip(link, ll2, status_list):
                data1.append(["https://www.itu.int/rec" + i['href'][1:],
                              list_l, title, id_subsection, Subsection,
                              i.get_text(strip=True),
                              j,
                              l])

        q.task_done()


async def worker_link(session, q):
    while True:
        url, list_l, title, id_subsection, Subsection, standart, title1, status = await q.get()
        # out1 = []
        soup = await get_soup(session, url)

        link_summary = soup.select('a:-soup-contains("Summary")')
        if link_summary:

            for i in link_summary:
                link = i.get('href')
                if link.endswith('.txt'):

                    out1.append(
                        "https://www.itu.int/rec" + link[1:]
                    )


                else:
                    out1.append(
                        "https://www.itu.int" + link
                    )




        else:
            out1.append(url)

        q.task_done()


async def worker_summary(session, q):
    while True:
        url = await q.get()

        soup = await get_soup(session, url)

        summary = soup.select('[class="MsoNormal"],[class="Normalaftertitle0"]')
        summary2 = soup.find('b')

        if url.endswith('.txt'):
            if summary2:
                summary_list.append({'summary': summary2.get_text().strip(),
                                     'URL': url})
            else:
                summary_list.append({'summary': '', 'URL': url})
        else:

            if summary:

                text = [''.join(b.get_text(strip=True) for b in summary)]
                for ki in text:
                    summary_list.append({'summary': ki, 'URL': url})
            else:
                summary_list.append({'summary': '', 'URL': url})

        q.task_done()


async def main():
    url = "https://www.itu.int/pub/T-REC"

    async with aiohttp.ClientSession() as session:
        soup = await get_soup(session, url)

        table = soup.find_all('table')[8]
        links = soup.select('td[width="28"] a')
        cols = table.select('td')

        committees = []
        for a, t in zip(links, cols[1::2]):
            committees.append(
                [
                    "https://www.itu.int" + a["href"],
                    a.get_text(strip=True),
                    t.get_text(strip=True)

                ]
            )

        queue = asyncio.Queue(maxsize=16)

        # Phase 1 - Get 6156 links:

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

        tasks = []

        # create 16 workers that will process data in parallel
        for i in range(16):
            task = asyncio.create_task(worker_iso(session, queue))
            tasks.append(task)

        # put some data to worker queue

        for c in tqdm.tqdm(out):
            await queue.put(c)

        # wait for all data to be processed
        await queue.join()

        # cancel all worker tasks
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

        # # # # Phase 3 - Get content 18096 links:

        tasks = []

        # create 16 workers that will process data in parallel
        for i in range(16):
            task = asyncio.create_task(worker_link(session, queue))
            tasks.append(task)

        # put some data to worker queue
        for c in tqdm.tqdm(data1):
            await queue.put(c)

        # wait for all data to be processed
        await queue.join()

        # cancel all worker tasks
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

        tasks = []

        # create 16 workers that will process data in parallel
        for i in range(16):
            task = asyncio.create_task(worker_summary(session, queue))
            tasks.append(task)

        # put some data to worker queue
        for c in tqdm.tqdm(out1):
            await queue.put(c)

        # wait for all data to be processed
        await queue.join()

        # cancel all worker tasks
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

        cache = {}
        for dct in chain(iso_data, summary_list):
            b = dct["URL"]
            cache.setdefault(b, dct).update(dct)

        temp_csv = list(cache.values())

        with open(f"l_task4.csv", "w") as file:
            writer = csv.writer(file)
            writer.writerow(
                ("b",
                 "level_1",
                 "level_2",
                 "level_3",
                 "iso_standart",
                 "stage",
                 "ics",
                 "abstract"
                 )
            )
        for isp in temp_csv:
            with open(f"l_task4.csv", "a") as file:
                writer = csv.writer(file)
                writer.writerow(
                    (
                        isp["b"],
                        isp["level_1"],
                        isp["level_2"],
                        isp["level_3"],
                        isp["iso_standart"],
                        isp["stage"],
                        isp["ics"],
                        isp["abstract"],
                    )
                )


if __name__ == "__main__":
    asyncio.run(main())
