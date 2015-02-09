import grequests
from grequests import put
import json


gtfs1 = 'http://169.53.140.166:5000'
gtfs2 = 'http://198.11.198.18:5000'
gtfs3 = 'http://198.11.198.19:5000'

h = {'content-type': 'application/json'}

reqs = [
    put(gtfs1+'/get_data', headers=h, data=json.dumps({'start': 0, 'end': 33})),
    put(gtfs2+'/get_data', headers=h, data=json.dumps({'start': 33, 'end': 66})),
    put(gtfs3+'/get_data', headers=h, data=json.dumps({'start': 66, 'end': 100}))
]

grequests.map(reqs)
