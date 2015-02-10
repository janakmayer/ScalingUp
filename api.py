from flask import Flask
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.restful.utils import cors
from flask.ext.runner import Runner
from api_functions import download_parse
import os
import json

app = Flask(__name__)
runner = Runner(app)
api = Api(app)
api.decorators = [cors.crossdomain(origin='*')]

parser = reqparse.RequestParser()
parser.add_argument('start', type=int)
parser.add_argument('end', type=int)
parser.add_argument('api_key', type=str)
parser.add_argument('word', type=str)

bi_gram = None


def load_bigram():
    with open('bi_gram.json', 'r') as json_file:
        global bi_gram
        bi_gram = json.load(json_file)
        json_file.close()

# API CLASSES
class GetData(Resource):
    def put(self):
        args = parser.parse_args(strict=True)
        if args['api_key'] == os.environ['API_KEY']:
            start = args['start']
            end = args['end']
            response = download_parse.run_downloader(start, end)
            return response
        else:
            return {"error": "invalid API key"}


class BiGram(Resource):
    def get(self):
        args = parser.parse_args(strict=True)
        global bi_gram
        if bi_gram == None:
            load_bigram()
        if args['api_key'] == os.environ['API_KEY']:
            word = args['word']
            return bi_gram.get(word, {})
        else:
            return {"error": "invalid API key"}


# API ROUTING
api.add_resource(GetData, '/data')
api.add_resource(BiGram, '/bigram')

if __name__ == '__main__':
    runner.run()