import asyncio
import pyppeteer
import time
import json
import pandas as pd


def is_linux():
    from platform import system
    return system() == 'Linux'


class PageSession(object):
    def __init__(self, url, headless=True, cookie_path=None):
        self.url = url
        self.cookie_path = cookie_path
        self.headless = headless
        self.width = 1440
        self.height = 1440

    async def __aenter__(self):
        params = {
            'headless': self.headless,
            'executablePath': '/usr/bin/google-chrome' if is_linux() else
            r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe',
            'args': ['--window-size=1440,1440']
        }
        self.browser = await pyppeteer.launch(params)
        context = await self.browser.createIncognitoBrowserContext()
        self.page = await context.newPage()
        client = await self.page.target.createCDPSession()
        await client.send('Emulation.setGeolocationOverride', {
            'accuracy': 100,
            'latitude': 40.7128,
            'longitude': 74.0060
        })
        tasks = [
            asyncio.ensure_future(self.page.setJavaScriptEnabled(True)),
            asyncio.ensure_future(self.page.setCacheEnabled(False)),
            asyncio.ensure_future(self.page.setViewport({"width": self.width, "height": self.height}))
        ]
        await asyncio.wait(tasks)
        if self.cookie_path is not None:
            with open(self.cookie_path) as f:
                cookies = json.load(f)
                for item in cookies:
                    await self.page.setCookie(item)
        await self.page.goto(self.url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.browser.close()


def update_dictionary(key, value, json_path):
    new_data = {}
    new_data[key] = value
    with open(json_path, 'r+') as f:
        file_data = json.load(f)
        file_data.update(new_data)
        f.seek(0)
        json.dump(file_data, f, indent=4)


async def process_category(page, link):
    path = "https://stockx.com/funko-pop/{}".format(link)
    await page.goto(path)
    # close the geolocation window
    await page.click("#chakra-modal-1 .chakra-modal__close-btn")
    last_page = 1
    page_numbers = await page.evaluate('() => {'
                                       """buttons = Array.from(document.querySelectorAll('a[class="css-1sg3yt8-PaginationButton"]'));"""
                                       'return buttons.map(button => button.textContent);'
                                       '}')
    if len(page_numbers) > 0:
        last_page = int(page_numbers[-1])

    item_hrefs = []
    for i in range(1, last_page+1):
        if i > 1:
            await page.goto(path + '?page={}'.format(i))
            await page.click("#chakra-modal-1 .chakra-modal__close-btn")
        elements = await page.querySelectorAll('.tile.browse-tile')
        for element in elements:
            item_href = await page.evaluate('(element) => element.querySelector("a").href', element)
            item_hrefs.append(item_href)
    return item_hrefs


async def process_item(page, link):
    await page.goto(link)
    # close the geolocation window
    #btn = await page.querySelector(".chakra-modal__close-btn")
    #if btn:
    #    await btn.click()
    result = {}
    product_name = await page.evaluate("""document.querySelector('h1[data-testid="product-name"]').textContent""")
    product_ticker = await page.evaluate("""document.querySelector('span[data-testid="product-ticker"]').textContent""")
    #await page.focus('#site-footer')
    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
    button = await page.querySelector("""button[class='btn']""")
    if button:
        await button.click()
    trs = await page.querySelectorAll('table tbody tr')
    sales = []
    for tr in trs:
        tds = await tr.querySelectorAll('td')
        sale = dict()
        sale['price'] = await page.evaluate('(element)=>element.textContent', tds[0])
        sale['date'] = await page.evaluate('(element)=>element.textContent', tds[1])
        sale['time'] = await page.evaluate('(element)=>element.textContent', tds[2])
        sales.append(sale)
    release_date_elem = await page.querySelector('span[data-testid="product-detail-release date"]')
    release_date = None
    if release_date_elem:
        release_date = await page.evaluate('(element)=>element.textContent', release_date_elem)
    result['product_name'] = product_name
    result['product_ticker'] = product_ticker
    result['sales'] = sales
    result['release_date'] = release_date.strip() if release_date is not None else None
    return result


async def main():
    home_page = "https://google.com"
    cookie_path = 'stockx_cookie_en.json'
    categories_path = 'funko_pop_categories.json'
    item_links_path = 'funko_pop_item_links.json'
    item_details_path = 'funko_pop_item_details.json'
    cleaned_data_path = 'data2.csv'
    skip_item_links = True
    skip_item_details = True
    async with PageSession(home_page, False, cookie_path) as page_session:
        page = page_session.page
        # close the geolocation window
        button = await page.querySelector("#chakra-modal-1 .chakra-modal__close-btn")
        if button:
            await button.click()
        time.sleep(1)

        # get categories
        categories = None
        with open(categories_path) as f:
            categories = json.load(f)

        # get item links
        if not skip_item_links:
            for category, links in categories.items():
                category_item_links = []
                for link in links:
                    item_links = await process_category(page, link)
                    category_item_links += item_links
                update_dictionary(category, category_item_links, item_links_path)

        # get item_details
        if not skip_item_details:
            with open(item_links_path) as f:
                saved_item_links = set()
                with open(item_details_path) as f2:
                    item_details = json.load(f2)
                    saved_item_links = set(item_details.keys())
                print("save item size: {}".format(len(saved_item_links)))
                category_to_item_links = json.load(f)
                for category, links in category_to_item_links.items():
                    for link in links:
                        if link not in saved_item_links:
                            link_details = await process_item(page, link)
                            link_details['category'] = category
                            update_dictionary(link, link_details, item_details_path)
                            time.sleep(5)

    # clean data
    data = {
        "product_name": [],
        "product_ticker": [],
        "category": [],
        "release_date": [],
        "sale_count": [],
        "average_sale_price": []
    }
    with open(item_details_path) as f2:
        item_details = json.load(f2)
        for k, v in item_details.items():
            data['product_name'].append(v['product_name'].strip())
            data['product_ticker'].append(v['product_ticker'].strip())
            data['category'].append(v['category'])
            data['release_date'].append(v['release_date'])
            data['sale_count'].append(len(v['sales']))
            if len(v['sales']) > 0:
                count = 0
                sum = 0
                for sale in v['sales']:
                    count += 1
                    sum += int(sale['price'].replace('$', '').replace(',', ''))
                avg_price = float(sum) / count
                data['average_sale_price'].append(avg_price)
            else:
                data['average_sale_price'].append(None)
    df = pd.DataFrame(data)
    df = df[df['sale_count'] > 0]
    df.to_csv(cleaned_data_path, index=False)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
