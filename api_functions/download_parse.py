from StringIO import StringIO
from zipfile import ZipFile
import contextlib
import csv
import json
from urllib import urlopen
from multiprocessing import Pool
import os


URL_TEMPLATE = 'http://storage.googleapis.com/books/ngrams/books/{fname}'
FILE_TEMPLATE = 'googlebooks-eng-all-2gram-20090715-{index}.csv'


def download_preprocess(index):
    f = FILE_TEMPLATE.format(index=index)
    f_name = f +".zip"
    url = URL_TEMPLATE.format(fname=f_name)
    print 'Retrieving: ' + f_name + '\n'
    req = urlopen(url)
    zipfile = ZipFile(StringIO(req.read()))
    bi_gram = {}
    print 'JSONizing: ' + f_name + '\n'
    with contextlib.closing(zipfile.open(f)) as csv_file:
        reader = csv.DictReader(csv_file, delimiter='\t', skipinitialspace=True, quoting=csv.QUOTE_NONE,
                                fieldnames=['ngram', 'year', 'match_count', 'page_count', 'volume_count'])
        try:
            for row in reader:
                try:
                    w1, w2 = row['ngram'].split()
                    count = int(row['match_count'])
                    bi_gram[w1] = bi_gram.get(w1, {})
                    bi_gram[w1][w2] = bi_gram[w1].get(w2, 0) + count
                except:
                    pass  # The only rows that will be skipped will be ones with special characters (non English)

            jfile = str(index) + '.json'
            with open(jfile, 'w') as json_file:
                json.dump(bi_gram, json_file)
                json_file.close()

            print 'Successfully downloaded and JSONized: ' + f + '  as: ' + jfile + '\n'

        except:
            print "there was a problem with this file", f


def run_downloader(start, end):
        index_list = range(start, end)
        pool = Pool()

        # Call the download function once for each index number, feeding it the index number as an argument
        # This will take a while because it is going to both download and JSONize all the data
        try:
            pool.map(download_preprocess, index_list)
        except KeyboardInterrupt:
            print "Interupted download and processing"

        # Finally do secondary pre-processing - combine the different JSON files
        # produced by all of the different threads into one single JSON file per CCI - this is quick
        bi_gram = {}
        for index in index_list:
            jfile = str(index) + '.json'
            json_data = open(jfile)
            data = json.load(json_data)
            for k,v in data.iteritems():
                bi_gram[k] = bi_gram.get(k, {})
                for kk, vv in v.iteritems():
                    bi_gram[k][kk] = vv

        with open('bi_gram.json', 'w') as json_file:
            json.dump(bi_gram, json_file)
            json_file.close()

        for index in index_list:
            jfile = str(index) + '.json'
            os.remove(jfile)  # Delete the temporary individual json files once bi_gram.json has been written

        print "successfully wrote combined all output in bi_gram.json"

        return {"success": "download completed"}