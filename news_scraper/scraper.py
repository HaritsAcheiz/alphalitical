from httpx import AsyncClient
import pandas as pd
from selectolax.parser import HTMLParser
from dataclasses import dataclass, field
import asyncio
import os
from typing import List
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin
import json

limit = asyncio.Semaphore(20)


@dataclass
class NewsScraper:
    base_urls: List[str] = field(default_factory=list)
    user_agent: str = 'Chrome/125.0.0.0 Safari/537.36'
    max_retries: int = 3
    timeout: int = 30

    async def fetch(self, url, params, payload=None):
        if payload:
            headers = {
                'x-algolia-api-key': 'a74cdcfcc2c69b5dabb4d13c4ce52788',
                'x-algolia-application-id': 'U2CIAZRCAD',
                'Referer': 'https://www.tempo.co/',
                'content-type': 'application/x-www-form-urlencoded'
            }
            async with AsyncClient(headers=headers, timeout=self.timeout) as aclient:
                async with limit:
                    try:
                        response = await aclient.post(
                            url,
                            follow_redirects=True,
                            params=params,
                            json=payload
                        )
                        print(f"Fetched {url} - Status: {response.status_code}")
                        response.raise_for_status()
                        return url, response.text
                    except Exception as e:
                        print(f"Error fetching {url}: {e}")
                        raise
        else:
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

    async def fetch_with_retries(self, url, params=None, payload=None):
        for attempt in range(self.max_retries):
            try:
                return await self.fetch(url, params=params, payload=payload)
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == self.max_retries - 1:
                    print(f"Max retries reached for {url}. Skipping...")
                    return url, None
                await asyncio.sleep(2 ** attempt)

    async def fetch_all(self, urls, mode='news', search_terms='', last_date=''):
        tasks = []
        for url in urls:
            payload = None
            if url == 'https://search.kompas.com/search':
                params = {
                    'q': search_terms,
                    'site_id': 'all',
                    'last_date': last_date,
                    'sort': 'latest'
                }
            elif url == 'https://www.detik.com/search/searchall':
                params = {
                    'query': search_terms,
                    'result_type': 'latest',
                    'fromdatex': datetime.strptime(last_date, '%Y-%m-%d').strftime('%d/%m/%Y'),
                    'todatex': (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%d/%m/%Y'),
                }
            elif url == 'https://www.tempo.co/search':
                params = {
                    'q': search_terms,
                    'page': 1,
                    'x-algolia-agent': 'Algolia for JavaScript (4.24.0); Browser',
                }

                payload = {
                    "query":search_terms,
                    "filters":"NOT unpublished_at",
                    "hitsPerPage":10,
                    "page":0
                }

                url = 'https://u2ciazrcad-1.algolianet.com/1/indexes/production_articles/query'
            elif url == 'https://www.cnnindonesia.com/search':
                params = {
                    'query': search_terms,
                    'idtype': 1,
                    'start': 0,
                    'limit': 200,
                    'fromdate': datetime.strptime(last_date, '%Y-%m-%d').strftime('%Y/%m/%d'),
                    'todate': (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y/%m/%d'),
                }
                url = 'https://www.cnnindonesia.com/api/v3/search'
            elif url == 'https://www.liputan6.com/search':
                params = {
                    'q': search_terms,
                    'order': 'latest',
                    'channel_id': '',
                    'from_date': datetime.strptime(last_date, '%Y-%m-%d').strftime('%d/%m/%Y'),
                    'to_date': (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%d/%m/%Y'),
                    'type': 'all'
                }
            else:
                params = {}    
            task = asyncio.create_task(self.fetch_with_retries(url, params, payload=payload))
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
                if 'kompas.com' in url:
                    tree = HTMLParser(html)
                    record = {}
                    record['source'] = 'kompas'
                    record['title'] = tree.css_first('title').text(strip=True)
                    record['url'] = url
                    record['published_at'] = tree.css_first('meta[name="content_PublishedDate"]').attributes.get('content', '')
                    record['scraped_at'] = datetime.now(timezone(timedelta(hours=7))).isoformat()
                    records.append(record)
                elif 'detik.com' in url:
                    tree = HTMLParser(html)
                    record = {}
                    record['source'] = 'detik'
                    record['title'] = tree.css_first('h1.detail__title').text(strip=True)
                    record['url'] = url
                    record['published_at'] = tree.css_first('meta[itemprop="datePublished"]').attributes.get('content', '')
                    record['scraped_at'] = datetime.now(timezone(timedelta(hours=7))).isoformat()
                    records.append(record)
                elif 'tempo.co' in url:
                    tree = HTMLParser(html)
                    record = {}
                    record['source'] = 'tempo'
                    data = tree.css_first('script[type="application/ld+json"]').text(strip=True)
                    json_data = json.loads(data)
                    record['title'] = json_data.get('headline', '')
                    record['url'] = url
                    record['published_at'] = json_data.get('datePublished', '')
                    record['scraped_at'] = datetime.now(timezone(timedelta(hours=7))).isoformat()
                    records.append(record)
                elif 'cnnindonesia.com' in url:
                    tree = HTMLParser(html)
                    record = {}
                    record['source'] = 'cnnindonesia'
                    data = tree.css_first('script[type="application/ld+json"]').text(strip=True)
                    json_data = json.loads(data)
                    record['title'] = json_data.get('headline', '')
                    record['url'] = url
                    record['published_at'] = json_data.get('datePublished', '')
                    record['scraped_at'] = datetime.now(timezone(timedelta(hours=7))).isoformat()
                    records.append(record)
                elif 'liputan6.com' in url:
                    tree = HTMLParser(html)
                    record = {}
                    record['source'] = 'liputan6'
                    record['title'] = tree.css_first('meta[property="og:title"]').attributes.get('content', '')
                    record['url'] = url
                    record['published_at'] = tree.css_first('meta[property="article:published_time"]').attributes.get('content', '')
                    record['scraped_at'] = datetime.now(timezone(timedelta(hours=7))).isoformat()
                    records.append(record)
                else:
                    continue
            
            daily_records = pd.DataFrame(records)
            os.makedirs('data/staging', exist_ok=True)
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
        news_urls = []
        if result[0] == 'https://search.kompas.com/search':
            tree = HTMLParser(result[1])
            news_elems = tree.css('a.article-link')
                
        elif result[0] == 'https://www.detik.com/search/searchall':
            tree = HTMLParser(result[1])
            news_elems = tree.css('h3.media__title > a.media__link')
            
        elif result[0] == 'https://u2ciazrcad-1.algolianet.com/1/indexes/production_articles/query':
            news_elems = json.loads(result[1]).get('hits', [])

        elif result[0] == 'https://www.cnnindonesia.com/api/v3/search':
            news_elems = json.loads(result[1]).get('data', [])
        elif result[0] == 'https://www.liputan6.com/search':
            tree = HTMLParser(result[1])
            news_elems = tree.css('a.headline__item')
        else:
            print('under construction')

        for elem in news_elems:
            if result[0] == 'https://u2ciazrcad-1.algolianet.com/1/indexes/production_articles/query':
                news_url = urljoin('https://www.tempo.co/', elem.get('canonical_url', ''))
            elif result[0] == 'https://www.cnnindonesia.com/api/v3/search':
                news_url = elem.get('url', '')
            else:
                news_url = elem.attributes.get('href')
            news_urls.append(news_url)

        return news_urls

    def get_news_urls(self, search_terms, last_date):
        news_urls = asyncio.run(scraper.fetch_all(self.base_urls, mode='search', last_date=last_date, search_terms=search_terms))

        return news_urls

    def get_news(self, news_urls):
        asyncio.run(scraper.fetch_all(news_urls, mode='news'))

if __name__ == '__main__':
    news_portal = ['https://search.kompas.com/search', 'https://www.detik.com/search/searchall', 'https://www.tempo.co/search', 'https://www.cnnindonesia.com/search', 'https://www.liputan6.com/search']
    search_terms = 'purbaya'
    last_date = (datetime.now(timezone(timedelta(hours=7))) + timedelta(days=-1)).strftime('%Y-%m-%d')
    scraper = NewsScraper(base_urls=news_portal)
    news_urls = scraper.get_news_urls(search_terms=search_terms, last_date=last_date)
    scraper.get_news(news_urls)