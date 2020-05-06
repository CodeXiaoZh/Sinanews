# 新浪新闻分布式爬虫
#Author : zhangxx
import re
import traceback

import cchardet
import time
import json
import asyncio
import urllib.parse as urlparse
import aiohttp
from bs4 import BeautifulSoup


class CrawlerClient:
    def __init__(self, ):
        self._workers = 0
        self.workers_max = 10
        self.server_host = 'localhost'
        self.server_port = 8000
        self.headers = {'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; '
                                       'Windows NT 6.1; Win64; x64; Trident/5.0)')}

        self.loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)
        self.session = aiohttp.ClientSession(loop=self.loop)

    async def downloader(self,session,url,timeout=10,headers=None,binary=False):
        _headers = {
            'User-Agent': ('Mozilla/5.0 (compatible; MSIE 9.0; '
                           'Windows NT 6.1; Win64; x64; Trident/5.0)'),
        }
        redirected_url = url
        if headers :
            _headers = headers
        try:
            async with session.get(url, headers=_headers, timeout=timeout) as response:
                status = response.status
                content = await response.read()
                if binary:
                    html = content
                else:
                    encoding = cchardet.detect(content)['encoding']
                    html = content.decode(encoding)
            redirected_url = str(response.url)
        except:
            traceback.print_exc()
            msg = 'failed download: {}'.format(url)
            print(msg)
            if binary:
                html = b''
            else:
                html = ''
            status = 0
        return status,html,redirected_url

    async def get_urls(self,):
        count = self.workers_max - self.queue.qsize()
        if count <= 0:
            print('no need to get urls this time')
            return None
        url = 'http://%s:%s/task?count=%s' % (
            self.server_host,
            self.server_port,
            count
        )
        try:
            async with self.session.get(url, timeout=3) as response:
                if response.status not in [200, 201]:
                    return
                jsn = await response.text()
                urls = json.loads(jsn)
                msg = ('get_urls()  to get [%s] but got[%s], @%s') % (
                    count, len(urls),
                time.strftime('%Y-%m-%d %H:%M:%S'))
                print(msg)
                for kv in urls.items():
                    await self.queue.put(kv)
                print('queue size:', self.queue.qsize(), ', _workers:', self._workers)
        except:
            traceback.print_exc()
            return

    async def send_result(self, result):
        url = 'http://%s:%s/task' % (
            self.server_host,
            self.server_port
        )
        try:
            async with self.session.post(url, json=result, timeout=3) as response:
                return response.status
        except:
            traceback.print_exc()
            pass

    def save_html(self, url, html):
        print('saved:', url, len(html))

    async def set_hurls(self,url):
        urls = []
        rule1 = 'https://news.sina.com.cn/c.*'
        rule2 = 'https://news.sina.com.cn/w.*'
        status,html,redirected_url = await self.downloader(self.session,url=url)
        soup = BeautifulSoup(html,'lxml')
        body = soup.body
        alls = body.find_all(name='a')
        for all in alls:
            try:
                text = all.text
                url = all['href']
                if re.match(rule1,url) or re.match(rule2,url) and url not in urls :
                    urls.append(url)
                else:
                    pass
            except Exception as e:
                print(url)
        return urls

    async def process(self,url,ishub):
        if ishub:
            try:
                newurls = await self.set_hurls(url)
                status = 200
                html = ''
                redirected_url = url
            except:
                print(newurls)
        else:
            newurls = []
            status, html, redirected_url = await self.downloader(self.session,url)
        self.save_html(url, html)
        self._workers -= 1
        print('downloaded:', url, ', html:', len(html),', newurls:',len(newurls))
        result = {
            'url': url,
            'url_real':redirected_url ,
            'status': status,
            'newurls': newurls,
        }
        await self.send_result(result)

    async def loop_get_urls(self, ):
        print('loop_get_urls() start')
        while 1:
            await self.get_urls()
            await asyncio.sleep(1)

    async def loop_crawl(self, ):
        print('loop_crawl() start')
        asyncio.ensure_future(self.loop_get_urls())
        counter = 0
        while 1:
            item = await self.queue.get()
            url, url_level = item
            self._workers += 1
            counter += 1
            asyncio.ensure_future(self.process(url, url_level))

            if self._workers > self.workers_max:
                print('====== got workers_max, sleep 3 sec to next worker =====')
                await asyncio.sleep(3)

    def start(self):
        try:
            self.loop.run_until_complete(self.loop_crawl())
        except KeyboardInterrupt:
            print('stopped by yourself!')
            pass

if __name__ == '__main__':
    ant = CrawlerClient()
    ant.start()