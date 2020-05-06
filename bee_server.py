# 新浪新闻分布式爬虫
#Author : zhangxx

from sanic import Sanic
from sanic import response

from Urlpool import UrlPool

urlpool = UrlPool('crawl_urlpool')

#添加初始链接
heads_urls = ['https://news.sina.com.cn/china/','https://news.sina.com.cn/world/']

urlpool.set_heads(heads_urls,600)
print(urlpool.head_pool)

app = Sanic(__name__)

@app.listener('after_server_stop')
async def cache_urlpool(app, loop):
    global urlpool
    print('caching urlpool after_server_stop')
    del urlpool
    print('bye!')

@app.route('/task')
async def tast(request):
    count = request.args.get('count',10)
    try:
        count = int(count)
    except:
        count = 10
    urls = urlpool.pop(count)
    return response.json(urls)

@app.route('/task', methods=['POST', ])
async def task_post(request):
    result =request.json
    urlpool.set_status(result['url'], result['status'])
    if result['url_real'] != result['url']:
        urlpool.set_status(result['url_real'], result['status'])
    if result['newurls']:
        print('receive URLs:', len(result['newurls']))
        for url in result['newurls']:
            urlpool.add(url)
    return response.text('ok')

if __name__ == '__main__':
    app.run(host="localhost", port=8000, debug=False,workers=1)