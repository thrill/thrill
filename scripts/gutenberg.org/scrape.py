import argparse
import os
import urllib
import urlparse
import sys #getsizeof
from Queue import Queue
from threading import Thread

parser = argparse.ArgumentParser(description = "Downloads books from gutenberg.org")
parser.add_argument('-n', '--num_books', action = 'store', type = int, help = 'number of books to scrape')
parser.add_argument('-d', '--dest', action = 'store', type = str, help = 'destination directory (files are overwritten)')

base_url = "https://www.gutenberg.org/files/"
args = parser.parse_args()
total_size = 0
result = open(os.path.join(args.dest, 'gutenberg.0.' + str(args.num_books) + '.txt'), 'w')
q = Queue()
out_queue = Queue()
num_worker_threads = min(args.num_books, 4)

def append():
    global total_size
    while True:
        data = out_queue.get()
        total_size = total_size + sys.getsizeof(data)
        result.write(data)
        out_queue.task_done()

def download_thread():
    global total_size
    while True:
        out_queue.put(download(base_url, q.get()))
        q.task_done()

def download(base, book_id):
    url = urlparse.urljoin(base, str(book_id) + '/' + str(book_id) + '.txt')
    print url
    return urllib.urlopen(url).read()

for i in range(args.num_books):
    q.put(i)

for i in range(num_worker_threads):
    t = Thread(target = download_thread)
    t.daemon = True
    t.start()
t = Thread(target = append)
t.daemon = True
t.start()

q.join()
out_queue.join()
result.close()
sys.stdout.write('\n')
print 'downloaded: ' + str(total_size) + 'b'

