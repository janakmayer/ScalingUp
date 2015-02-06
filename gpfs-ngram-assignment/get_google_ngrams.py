"""Multithreaded google bigram downloader

This script uses workerpoool for multithreading, to enable more than one file to be downloaded at once
to speed the process of downloading bigram data from http://storage.googleapis.com/books/ngrams/books/datasetsv2.html
"""

import urllib
import sys
from multiprocessing import Pool

URL_TEMPLATE = 'http://storage.googleapis.com/books/ngrams/books/{fname}'
FILE_TEMPLATE = 'googlebooks-eng-all-2gram-20090715-{index}.csv.zip'


def download(index):
    fname = FILE_TEMPLATE.format(index=index)
    url = URL_TEMPLATE.format(fname=fname)
    print 'Retrieving: ' + fname + '\n'
    urllib.urlretrieve(url=url, filename=fname)
    print 'Successfully downloaded: ' + fname + '\n'


if __name__ == '__main__':
    start = int(sys.argv[1])  # first command line argument is starting index
    stop = int(sys.argv[2])  # second command line argument is stopping index
    index_list = range(start, stop)

    pool = Pool()
    # Call the download function once for each index number, feeding it the index number as an argument
    pool.map(download, index_list)