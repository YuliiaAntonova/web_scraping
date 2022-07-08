import csv

from itertools import chain
import tqdm
import asyncio
import aiohttp
from bs4 import BeautifulSoup

out = []
iso_data = []
iso_standarts = []
data_dict = []
t = []

async def get_soup(session, url):
    async with session.get(url=url) as resp:
        return BeautifulSoup(await resp.text(), "lxml")


async def worker(session, q):
    while True:
        url, link_name, title = await q.get()

        soup = await get_soup(session, url)

        links = soup.select('[data-title="ICS"] a')
        t.append(links)
        if links:

            for a in links:
                out.append([
                    "https://www.iso.org" + a["href"],
                    link_name,
                    title]
                )

        else:
            out.append([url, link_name, title])

        q.task_done()


async def worker_iso(session, q):
    while True:
        url, list_l, title = await q.get()
        soup = await get_soup(session, url)
        level_1 = 'ICS'
        level_3 = soup.find('title').text
        storeTable = soup.find_all("tr", {"ng-show": "pChecked || pChecked == null"})
        storeTable1 = soup.find_all("tr", {"ng-show": "uChecked || uChecked == null"})
        for j in storeTable:
            storeTable1.append(j)
        for i in storeTable1:
            t = i.find('div', {'class': 'entry-title'})
            iso = t.parent.get_text().strip()

            n = i.find('div', {'class': 'entry-summary'}).text

            a1 = i.find("a").attrs["href"]

            try:
                b1 = f"https://www.iso.org{a1}"
            except:
                b1 = "no"
            iso_standarts.append(b1)
            try:
                iso_standart = f'{iso}{n}'
            except:
                iso_standart = 'nothing'
            try:
                stage = i.find('td', {'data-title': 'Stage'}).text
            except:
                stage = 'nothing'
            try:
                tc = i.find('td', {'data-title': 'TC'}).text
            except:
                ics = 'nothing'
            iso_data.append(
                {'b': b1, 'level_1': level_1, 'level_2': list_l + '_' + title, 'level_3': level_3,
                 'iso_standart': iso_standart, 'stage': stage, 'ics': tc})

        q.task_done()


async def worker_link(session, q):
    while True:
        url = await q.get()
        soup = await get_soup(session, url)

        res = soup.find(id="product-details")
        abstract = res.find_all('div', attrs={'itemprop': 'description'})

        if abstract:
            for i in abstract:
                data_dict.append({'abstract': i.text, 'b': url})
        else:
            data_dict.append({'abstract': '', 'b': url})

        q.task_done()


async def main():
    url = "https://www.iso.org/standards-catalogue/browse-by-ics.html"

    async with aiohttp.ClientSession() as session:
        soup = await get_soup(session, url)
        print(url)
        titles = soup.select('td[data-title="Field"]')
        links = soup.select('td[data-title="ICS"] a')
        committees = []
        for a, t in zip(links, titles):
            committees.append(
                [
                    "https://www.iso.org" + a["href"],
                    a.get_text(strip=True),
                    t.get_text(strip=True),
                ]
            )

        queue = asyncio.Queue(maxsize=16)

        # Phase 1 - Get 653 links:

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

        # # Phase 3 - Get content 18096 links:

        tasks = []

        # create 16 workers that will process data in parallel
        for i in range(16):
            task = asyncio.create_task(worker_link(session, queue))
            tasks.append(task)

        # put some data to worker queue
        for c in tqdm.tqdm(iso_standarts):
            await queue.put(c)

        # wait for all data to be processed
        await queue.join()

        # cancel all worker tasks
        for task in tasks:
            task.cancel()

        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)
        #
        cache = {}
        for dct in chain(iso_data, data_dict):
            b = dct["b"]
            cache.setdefault(b, dct).update(dct)

        temp_csv = list(cache.values())


        with open(f"l_task5.csv", "w") as file:
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
            with open(f"l_task5.csv", "a") as file:
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
