import pycurl
import cStringIO
import cProfile
from time import time, sleep
import re
import threading
import collections

start_point = time()
traffic = 0
cntload = 0

START_URL = 'http://ru.wikipedia.org/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0'
INCLUDE_URL = 'ru.wikipedia.org'

def timeit(f, *lparam, **kparam):
    start = time()
    f(*lparam, **kparam)
    print 'execution time = %.2f ms' % ((time() - start) * 1000.)

def loadpage(url):
    global traffic, cntload
    if INCLUDE_URL not in url:
        return ''
    url = dns_cache(url)
#    print 'start load ', url
    try:
        buf = cStringIO.StringIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEFUNCTION, buf.write)
        start_t = time()
        c.perform()
        res = buf.getvalue()
        buf.close()
        traffic += len(res)
        cntload += 1.0
        print 'loaded', url, 'len =', len(res), 'bytes', ('all time = %.2f msec' % ((time() - start_t) * 1000.))
    except pycurl.error:
        return ''
    return res

def dns_cache(url):
    r = [
        ]
    for s, t in r:
        url = url.replace(s, t)
    return url

def links(url, page):
    if url[-1] != '/':
        url = url + '/'
    for urltype, suburl in re.findall(r'<[^>]*(href|src)="?([^"> \']+)"?[^<]*>', page, re.I | re.X):
        if 'data:' not in suburl:
            if '//' in suburl:
                if '://' not in suburl:
                    yield 'http:' + suburl
                else:
                    yield suburl
            elif suburl[0] != '/':
                yield url + suburl
            else:
                yield url + suburl[1:]

def load(url, depth):
    queue = collections.deque()
    queue.append((url, depth))
    global res
    res = {}
    def process(url, depth):
        page = loadpage(url)
        res[url] = page
        if depth > 0:
            for surl in links(url, page):
                queue.append((surl, depth - 1))
    def getind():
        for i in range(len(taskq)):
            if not taskq[i].isAlive():
                return i
        return 0
    taskq = []
    while True:
        while not queue and taskq:
            ind = getind()
            thread = taskq.pop(ind)
            thread.join()
        if not queue:
            break
        url, depth = queue.popleft()
        if len(taskq) > 60:
            ind = getind()
            thread = taskq.pop(ind)
            thread.join()
        if url not in res:
            thread = threading.Thread(target=process,args=(url,depth))
            thread.start()
            taskq.append(thread)
        if cntload:
            print 'q=', len(queue), ('speed = %.2f Kib/s, avg document size = %.2f Kib, cnt request = %d' % (traffic / (time() - start_point) / 1024., traffic / cntload / 1024., cntload))
def main():
    timeit(load,START_URL , 10)

main()
#cProfile.run('main()')
