from httpx import AsyncClient
import pandas as pd
from selectolax.parser import HTMLParser
from dataclasses import dataclass, field
import asyncio
import os
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
            records = []
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

                try:
                    record['published_at'] = tree.css_first('div.read__time').text(strip=True)
                except AttributeError:
                    record['published_at'] = tree.css_first('div.videoKG-date').text(strip=True)
                
                record['scraped_at'] = datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S')
                records.append(record)
            
            daily_records = pd.DataFrame(records)
            os.makedirs('data', exist_ok=True)
            daily_records.to_csv(
                f'data/staging/news_{datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")}.csv',
                mode='w',
                index=False
            )
        
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

if __name__ == '__main__':
    # 'https://www.detik.com/', 'https://www.tempo.co/', 'https://www.cnnindonesia.com/', 'https://www.liputan6.com/']
    news_portal = ['https://search.kompas.com/search']
    scraper = NewsScraper(base_urls=news_portal)
    news_urls = scraper.get_news_urls(search_terms='purbaya', last_date='2025-10-16')
    scraper.get_news(news_urls)