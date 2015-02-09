from flask import Flask
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.restful.utils import cors
from flask.ext.runner import Runner
from StringIO import StringIO
from zipfile import ZipFile
import contextlib
import csv
import json
from urllib import urlopen
from multiprocessing import Pool
import re

import sys


app = Flask(__name__)
runner = Runner(app)
api = Api(app)
api.decorators = [cors.crossdomain(origin='*')]

parser = reqparse.RequestParser()
parser.add_argument('start', type=int)
parser.add_argument('end', type=int)


URL_TEMPLATE = 'http://storage.googleapis.com/books/ngrams/books/{fname}'
FILE_TEMPLATE = 'googlebooks-eng-all-2gram-20090715-{index}.csv'


def download(index):
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
            i = 0
            for row in reader:
                i += 1
                # if not re.match(r'.*[\%\$\^\*\@\!\_\-\(\)\:\;\'\"\{\}\[\]].*', row['ngram']):
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
            print "there was a problem with this file", f, "this was the last row: ", row, "row number: ", i

def run_downloader(start, end):
        index_list = range(start, end)
        pool = Pool()

        # Call the download function once for each index number, feeding it the index number as an argument
        # This will take a while because it is going to both download and JSONize
        try:
            pool.map(download, index_list)
        except KeyboardInterrupt:
            print "Interupted download and processing"

        # Finally do secondary pre-processing - combine the different JSON files
        # produced by all of the different threads into one single JSON file per CCI
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
        print "successfully wrote combined all output in bi_gram.json"

        return {"success": "download completed"}

# API CLASSES
class GetData(Resource):
    def put(self):
        args = parser.parse_args(strict=True)
        start = args['start']
        end = args['end']
        return run_downloader(start, end)

# API ROUTING
api.add_resource(GetData, '/get_data')

if __name__ == '__main__':
    # runner.run()
    start = int(sys.argv[1])
    end = int(sys.argv[2])
    run_downloader(start, end)
    print "success: download completed"