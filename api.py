from flask import Flask
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.restful.utils import cors
from flask.ext.runner import Runner
from api_functions import download_parse
import os

app = Flask(__name__)
runner = Runner(app)
api = Api(app)
api.decorators = [cors.crossdomain(origin='*')]

parser = reqparse.RequestParser()
parser.add_argument('start', type=int)
parser.add_argument('end', type=int)
parser.add_argument('api_key', type=str)


# API CLASSES
class GetData(Resource):
    def put(self):
        args = parser.parse_args(strict=True)
        if args['api_key'] == os.environ['API_KEY']:
            start = args['start']
            end = args['end']
            return download_parse.run_downloader(start, end)
        else:
            return {"error": "invalid API key"}

# API ROUTING
api.add_resource(GetData, '/get_data')

if __name__ == '__main__':
    runner.run()