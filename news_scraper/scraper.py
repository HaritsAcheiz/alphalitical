import json
from httpx import AsyncClient, Client
from numpy import tile
import pandas as pd
from selectolax.parser import HTMLParser
from dataclasses import dataclass, field
import asyncio
from urllib.parse import urljoin
import sqlite3
import os
from urllib.parse import urlparse, parse_qs, urlunparse
from typing import List
from datetime import datetime, timezone, timedelta

limit = asyncio.Semaphore(20)


@dataclass
class NewsScraper:
    base_urls: List[str] = field(default_factory=list)
    user_agent: str = 'Chrome/125.0.0.0 Safari/537.36'
    max_retries: int = 3
    timeout: int = 30

    async def fetch(self, url, params):
        headers = {
            'user-agent': self.user_agent,
        }

        async with AsyncClient(headers=headers, timeout=self.timeout) as aclient:
            async with limit:
                try:
                    response = await aclient.get(
                        url,
                        follow_redirects=True,
                        params=params
                    )
                    print(f"Fetched {url} - Status: {response.status_code}")
                    response.raise_for_status()
                    return url, response.text
                except Exception as e:
                    print(f"Error fetching {url}: {e}")
                    raise

    async def fetch_with_retries(self, url, params):
        for attempt in range(self.max_retries):
            try:
                return await self.fetch(url, params)
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == self.max_retries - 1:
                    print(f"Max retries reached for {url}. Skipping...")
                    return url, None
                await asyncio.sleep(2 ** attempt)

    async def fetch_all(self, urls, mode='news', params=None):
        tasks = []
        for url in urls:
            task = asyncio.create_task(self.fetch_with_retries(url, params))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        if mode == 'news':
            try:
                # conn = sqlite3.connect("news_article.db")
                # curr = conn.cursor()
                # curr.execute(
                #     """
                #     CREATE TABLE IF NOT EXISTS news_article(
                #     source TEXT,
                #     title TEXT,
                #     url TEXT PRIMARY KEY,
                #     published_at TEXT,
                #     scraped_at TEXT
                #     )
                #     """
                # )

                for result in results:
                    if isinstance(result, Exception):
                        print(f"Skipping due to exception: {result}")
                        continue
                    url, html = result
                    if html is None:
                        print(f"No data for {url} after retries.")
                        continue
                    tree = HTMLParser(html)
                    record = {}
                    record['source'] = 'kompas'
                    record['title'] = tree.css_first('title').text(strip=True)
                    record['url'] = url

                    # published_at
                    input_string = tree.css_first('div.read__time').text(strip=True)
                    format_to_parse = "- %d/%m/%Y, %H:%M WIB"
                    naive_dt = datetime.strptime(input_string, format_to_parse)
                    wib_offset = timedelta(hours=7)
                    wib_tz = timezone(wib_offset)
                    aware_dt = naive_dt.replace(tzinfo=wib_tz)
                    iso_8601_string = aware_dt.isoformat()
                    record['published_at'] = iso_8601_string

                    # record['scraped_at'] =
                    print(record)
                #     curr.execute(
                #         "INSERT OR REPLACE INTO htmls (url, content) VALUES(?,?)",
                #         (url, content)
                #     )
                # conn.commit()
            except Exception as e:
                print(f"Error saving to database: {e}")
            finally:
                pass
                # conn.close()
        elif mode == 'search':
            news_urls = []
            for result in results:
                news_urls.extend(self.parse_news_urls(result))
            return news_urls
        else:
            print('Mode is not available')

    def parse_news_urls(self, result):
        if result[0] == 'https://search.kompas.com/search':
            tree = HTMLParser(result[1])
            news_elems = tree.css('a.article-link')
            news_urls = []
            for elem in news_elems:
                news_url = elem.attributes.get('href')
                news_urls.append(news_url)
            return news_urls
        else:
            print('under construction')

    def get_news_urls(self, search_terms, last_date):
        params = {
            'q': search_terms,
            'site_id': 'all',
            'last_date': last_date,
            'sort': 'latest'
        }

        news_urls = asyncio.run(scraper.fetch_all(self.base_urls, mode='search', params=params))

        return news_urls

    def get_news(self, news_urls):
        asyncio.run(scraper.fetch_all(news_urls, mode='news'))

        # for item in results:
        #     try:
        #         tree = HTMLParser(item)
        #         item_elems = tree.css('div#pagedisplay > ul > li')
        #         if not item_elems:
        #             item_elems = tree.css('div#page0 > li')
        #         sub_col_elems = tree.css('div.pop-cat > ul > li')
        #         if item_elems:
        #             try:
        #                 product_name = tree.css_first('h1').text(strip=True)
        #             except Exception:
        #                 print('product name not found')
        #             page_type = 'collection'
        #             for item_elem in item_elems:
        #                 item_urls.append(self.base_url + '/' + item_elem.css_first('a[itemprop="url"]').attributes.get('href'))
        #                 item_names.append(item_elem.css_first('span[itemprop="name"]').text(strip=True))
        #             item_urls = ';'.join(item_urls)
        #             item_names = ';'.join(item_names)
        #             sub_col_urls = ';'.join(sub_col_urls)
        #             sub_col_names = ';'.join(sub_col_names)
        #         elif not item_elems and sub_col_elems:
        #             try:
        #                 product_name = tree.css_first('h1').text(strip=True)
        #             except Exception:
        #                 print('product name not found')
        #             page_type = 'parent collection'
        #             for sub_col_elem in sub_col_elems:
        #                 sub_col_urls.append(self.base_url + '/' + sub_col_elem.css_first('a[itemprop="url"]').attributes.get('href'))
        #                 sub_col_names.append(sub_col_elem.text(strip=True))
        #             item_urls = ''
        #             item_names = ''
        #             sub_col_urls = ';'.join(sub_col_urls)
        #             sub_col_names = ';'.join(sub_col_names)
        #         else:
        #             try:
        #                 product_name = tree.css_first('h1').text(strip=True)
        #             except Exception:
        #                 print(f'product name not found {data[0]}')
        #             item_urls = ''
        #             item_names = ''
        #             sub_col_urls = ''
        #             sub_col_names = ''
        #             page_type = 'item'
        #     except Exception as e:
        #         print(f'error due to {e}')
        #         product_name = ''
        #         item_urls = ''
        #         item_names = ''
        #         sub_col_urls = ''
        #         sub_col_names = ''
        #         page_type = ''
        #     finally:
        #         collections.append((data[0], product_name, page_type, item_urls, item_names, sub_col_urls, sub_col_names))
        # collection_df = pd.DataFrame(columns=['product_url', 'product_name', 'page_type', 'item_urls', 'item_names', 'sub_col_urls', 'sub_col_names'], data=collections)
        # collection_df.to_csv('data/collections.csv', index=False)

    # def extract_image(self, text):
    #     pattern = r'url\(([^)]+)\)'
    #     match = re.search(pattern, text)
    #     if match:
    #         url = match.group(1)
    #     else:
    #         url = ''

    #     return url

    # def get_image(self):
    #     conn = sqlite3.connect("rctrucks.db")
    #     curr = conn.cursor()
    #     curr.execute("SELECT product_url, html FROM htmls")
    #     datas = curr.fetchall()
    #     products = []
    #     for data in datas:
    #         images = []
    #         print(data[1])
    #         tree = HTMLParser(data[1])
    #         image_elements = []
    #         main_img_elem = tree.css_first('div.detail-img > span > a')
    #         secondary_img_elems = tree.css('a.lightbox02')
    #         collection_img_elem = tree.css_first('div.inner-banner')
    #         if main_img_elem:
    #             image_elements.append(main_img_elem)
    #         if secondary_img_elems:
    #             for elem in secondary_img_elems:
    #                 image_elements.append(elem)
    #         if collection_img_elem:
    #             image_elements.append(collection_img_elem)
    #         if image_elements:
    #             for elem in image_elements:
    #                 image_href = elem.attributes.get('href')
    #                 if image_href:
    #                     images.append(image_href.strip())
    #                 else:
    #                     images.append(self.extract_image(elem.attributes.get('style')).strip())
    #             result = ';'.join(images)
    #         else:
    #             result = ''
    #         products.append((data[0], result))
    #     image_df = pd.DataFrame(columns=['product_url', 'images'], data=products)
    #     image_df.to_csv('data/images.csv', index=False)

    # def parse_images(self, desc):
    #     if pd.isna(desc):
    #         result = ''
    #     else:
    #         tree = HTMLParser(desc)
    #         origin_image_links = []
    #         images = tree.css('img')
    #         for image in images:
    #             origin_image_link = image.attributes.get('src').strip()
    #             origin_image_links.append(origin_image_link)
    #         result = ';'.join(origin_image_links)

    #     return result

    # def parse_docs(self, desc):
    #     if pd.isna(desc):
    #         result = ''
    #     else:
    #         tree = HTMLParser(desc)
    #         docs = tree.css('a')
    #         origin_doc_links = []
    #         if docs:
    #             for doc in docs:
    #                 origin_doc_link = doc.attributes.get('href')
    #                 if origin_doc_link:
    #                     origin_doc_links.append(origin_doc_link.strip())
    #                 else:
    #                     continue
    #             result = ';'.join(origin_doc_links)
    #         else:
    #             result = ''

    #     return result

    # def parse_videos(self, desc):
    #     if pd.isna(desc):
    #         result = ''
    #     else:
    #         tree = HTMLParser(desc)
    #         origin_video_links = []
    #         videos = tree.css('iframe')
    #         for video in videos:
    #             origin_video_link = video.attributes.get('src').strip()
    #             origin_video_links.append(origin_video_link)
    #         result = ';'.join(origin_video_links)

    #     return result

    # def image_link_correction(self, origin_image_links):
    #     origin_image_links = origin_image_links.strip()
    #     if 'system.netsuite.com/c.' in origin_image_links:
    #         parts = origin_image_links.split("/")
    #         account_id = parts[3].split(".")[1]
    #         parts[2] = f"{account_id}.app.netsuite.com"
    #         parts[-1] = parts[-1].replace(" ", "%20")
    #         actual_image_links = "/".join(parts)

    #     elif 'system.netsuite.com/core/media/media.nl' in origin_image_links:
    #         parsed_url = urlparse(origin_image_links)
    #         query_params = parse_qs(parsed_url.query)
    #         if 'c' in query_params:
    #             account_id = query_params['c'][0]
    #             netloc = f"{account_id}.app.netsuite.com"
    #             corrected_url = parsed_url._replace(netloc=netloc)
    #             actual_image_links = urlunparse(corrected_url)
    #         else:
    #             actual_image_links = origin_image_links

    #     elif ('ep.yimg.com/ty/cdn/gasscooters' in origin_image_links) or ('sep.yimg.com/ty/cdn/gasscooters' in origin_image_links):
    #         temp_image_links = origin_image_links.replace('sep.yimg.com', 'ep.turbifycdn.com')
    #         actual_image_links = temp_image_links.replace('ep.yimg.com', 'ep.turbifycdn.com')

    #     elif 'lib.store.yahoo.net/lib/gasscooters' in origin_image_links:
    #         actual_image_links = origin_image_links.replace('http://lib.store.yahoo.net/lib/gasscooters', 'https://sep.turbifycdn.com/ty/cdn/gasscooters')

    #     else:
    #         actual_image_links = origin_image_links

    #     return actual_image_links

    # def video_link_correction(self, origin_video_links):
    #     actual_video_links = origin_video_links.replace(
    #         'http://lib.store.yahoo.net/lib/gasscooters',
    #         'https://sep.turbifycdn.com/ty/cdn/gasscooters'
    #     )

    #     return actual_video_links

    # def doc_link_correction(self, origin_doc_links):
    #     if ('ep.yimg.com/ty/cdn/gasscooters' in origin_doc_links) or ('sep.yimg.com/ty/cdn/gasscooters' in origin_doc_links):
    #         temp_doc_links = origin_doc_links.replace('sep.yimg.com', 'ep.turbifycdn.com')
    #         actual_doc_links = temp_doc_links.replace('ep.yimg.com', 'ep.turbifycdn.com')
    #     elif 'lib.store.yahoo.net/lib/gasscooters' in origin_doc_links:
    #         actual_doc_links = origin_doc_links.replace(
    #             'http://lib.store.yahoo.net/lib/gasscooters',
    #             'https://sep.turbifycdn.com/ty/cdn/gasscooters'
    #         )
    #     else:
    #         actual_doc_links = origin_doc_links

    #     return actual_doc_links

    # def download_images(self, actual_image_links):
    #     if pd.isna(actual_image_links) or actual_image_links == '':
    #         pass
    #     else:
    #         actual_image_links = actual_image_links.split(';')
    #         for link in actual_image_links:
    #             save_path = link.split('/')[-1]
    #             file_path = os.path.join('data/downloads/', save_path)
    #             if not os.path.isfile(file_path):
    #                 try:
    #                     with Client(follow_redirects=True) as client:
    #                         response = client.get(link)

    #                     if response.status_code == 200:
    #                         with open(f'data/downloads/{save_path}', 'wb') as file:
    #                             file.write(response.content)
    #                         print(f"Image successfully downloaded and saved to {save_path}")
    #                     else:
    #                         print(f"Failed to download image. Status code: {response.status_code}")
    #                 except Exception as e:
    #                     print(f"An error occurred: {e}")
    #             else:
    #                 print(f'{save_path} is already exist')

    # def get_image_desc(self):
    #     df = self.source.copy()
    #     df['origin_image_links'] = df['caption'].apply(self.parse_images)
    #     df['origin_image_links'] = df['origin_image_links'].apply(lambda x: x.split(';'))
    #     # df['actual_image_links'] = df['actual_image_links'].apply(lambda x: x.split(';'))
    #     df = df.explode('origin_image_links')
    #     df['actual_image_links'] = df['origin_image_links'].apply(self.image_link_correction)
    #     df['filename'] = df['origin_image_links'].apply(lambda x: x.split('/')[-1].split('?')[0] if 'media.nl' not in x else x.split('id=')[-1].split('&')[0])
    #     df['file_type'] = df['filename'].apply(lambda x: '' if pd.isna(x) or x == '' else 'IMAGE')
    #     # df['actual_image_links'].apply(self.download_images)

    #     result = df[['product-url', 'filename', 'file_type', 'origin_image_links', 'actual_image_links']]
    #     result = result.explode(['origin_image_links', 'actual_image_links'])
    #     result.to_csv('data/description_image_link.csv', index=False)

    # def get_doc_desc(self):
    #     df = self.source.copy()
    #     df['origin_doc_links'] = df['caption'].apply(self.parse_docs)
    #     df['origin_doc_links'] = df['origin_doc_links'].apply(lambda x: x.split(';'))
    #     df = df.explode('origin_doc_links')
    #     df['actual_doc_links'] = df['origin_doc_links'].apply(self.doc_link_correction)
    #     df['filename'] = df['origin_doc_links'].apply(lambda x: x.split('/')[-1].split('?')[0] if 'media.nl' not in x else x.split('id=')[-1].split('&')[0])
    #     df['file_type'] = df['filename'].apply(lambda x: '' if pd.isna(x) or x == '' else 'FILE')
    #     result = df[['product-url', 'filename', 'file_type', 'origin_doc_links', 'actual_doc_links']]
    #     result = result.explode(['origin_doc_links', 'actual_doc_links'])
    #     result.to_csv('data/description_doc_link.csv', index=False)

    # def get_video_desc(self):
    #     df = self.source.copy()
    #     df['origin_video_links'] = df['caption'].apply(self.parse_videos)
    #     df['origin_video_links'] = df['origin_video_links'].apply(lambda x: x.split(';'))
    #     df = df.explode('origin_video_links')
    #     df['actual_video_links'] = df['origin_video_links'].apply(self.video_link_correction)
    #     df['filename'] = df['origin_video_links'].apply(lambda x: x.split('/')[-1].split('?')[0] if 'media.nl' not in x else x.split('id=')[-1].split('&')[0])
    #     df['file_type'] = df['filename'].apply(lambda x: '' if pd.isna(x) or x == '' else 'VIDEO')
    #     result = df[['product-url', 'filename', 'file_type', 'origin_video_links', 'actual_video_links']]
    #     result = result.explode(['origin_video_links', 'actual_video_links'])
    #     result.to_csv('data/description_video_link.csv', index=False)


if __name__ == '__main__':
    # 'https://www.detik.com/', 'https://www.tempo.co/', 'https://www.cnnindonesia.com/', 'https://www.liputan6.com/']
    news_portal = ['https://search.kompas.com/search']
    scraper = NewsScraper(base_urls=news_portal)
    news_urls = scraper.get_news_urls(search_terms='purbaya', last_date='2025-10-16')
    scraper.get_news(news_urls)