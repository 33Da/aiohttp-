import asyncio
import re
import aiohttp
import aiomysql
from pyquery import PyQuery

start_url = "http://www.jobbole.com/"
waitting_url = []
seen_urls = set()  # 去重
stopping = False

#限制并发度
sem = asyncio.Semaphore(3)


# 抓取网页
async def fetch(url, session):
    async with sem:
        try:
            async with session.get(url) as resp:
                if resp.status in [200, 201]:
                    data = await  resp.text()

                    print(await resp.text())
                    return data
        except Exception as e:
            print(e)


#
async def main(loop):
    # 等待mysql建立好
    pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='root',
                                      db='aiomysql_test', loop=loop,
                                      charset='utf8', autocommit=True)

    #发送http请求
    async with aiohttp.ClientSession() as session:
        html = await fetch(start_url, session)
        seen_urls.add(start_url)
        extract_url(html)

    asyncio.ensure_future(consumer(pool))


# 获取文章详情并入库
async def article_handler(url, session, pool):
    html = await  fetch(url, session)
    seen_urls.add(url)
    extract_url(html)
    pq = PyQuery(html)
    title = pq('title').text()

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 42;")
            insert_sql = "insert into article(title) values('{}')".format(title)
            await cur.execute(insert_sql)


# 对waitting_url进行操作
async def consumer(pool):
    async with aiohttp.ClientSession() as session:
        while not stopping:
            #防止这里运行过快waitting_url为空,判断一下
            if len(waitting_url) == 0:
                await asyncio.sleep(0.5)
                continue

            url = waitting_url.pop()
            print('start get url: {}'.format(url))

            #如果链接匹配
            if re.match('http://.*?jobbole.com/\d+/', url):
                if url not in seen_urls:
                    #获取文章详情
                    asyncio.ensure_future(article_handler(url, session, pool))

            #如果不匹配
            else:
                if url not in seen_urls:
                    asyncio.ensure_future(init_urls(url, session))


# 提取网页上的url
def extract_url(html):
    urls = []
    pq = PyQuery(html)
    # 爬取所有a链接
    for link in pq.items("a"):
        # 提取href属性
        url = link.attr('href')
        if url and url.startswith('http') and url not in seen_urls:  # 判断是否重复或是否是http
            urls.append(url)
            waitting_url.append(url)
    return urls

# 控制爬取队列
async def init_urls(url, session):
    html = await fetch(url, session)
    seen_urls.add(url)
    extract_url(html)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main(loop))
    loop.run_forever()
