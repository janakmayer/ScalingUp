import urllib
import sys


def get_data(start, stop):
    index_list = range(start, stop)
    url_template = 'http://storage.googleapis.com/books/ngrams/books/{}'
    file_template = 'googlebooks-eng-all-2gram-20090715-{index}.csv.zip'
    for index in index_list:
        fname = file_template.format(index=index)
        url = url_template.format(fname)
        print 'Currently retrieving: ' + fname
        urllib.urlretrieve(url=url, filename=fname)


if __name__ == '__main__':
    get_data(sys.argv[0], sys.argv[1])