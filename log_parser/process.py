# -*- coding=utf-8 -*-
import re
import heapq
from collections import namedtuple, defaultdict, Counter
#from urlparse import urlparse

Match = namedtuple('Match', ('time', 'id', 'type', 'additional'))

PATTERN = (
    '(?P<TIME>\d{16})\t' # only for dates after 09.09.2001
    '(?P<ID>\d{1,8})\t' 
    '(?P<TYPE>(StartRequest|BackendConnect|BackendRequest|BackendOk|BackendError|StartMerge|StartSendResult|FinishRequest))'
    '(?:\t(?P<ADDITIONAL>.*))?'
)

ADDITIONAL_PATTERNS = {
    'StartRequest': None,
    'BackendConnect': '(?P<GR>\d{1,8})\t(?P<URL>.*)',
    'BackendRequest': '(?P<GR>\d{1,8})',
    'BackendOk': '(?P<GR>\d{1,8})',
    'BackendError': '(?P<GR>\d{1,8})\t(?P<ERROR>.*)',
    'StartMerge': None,
    'StartSendResult': None,
    'FinishRequest': None,
}


def get_processor(additional_patterns={}):
    additional_regex = {k: re.compile(v) for k, v in additional_patterns.iteritems() if v}
    additional_match = lambda t, x: additional_regex[t].match(x).groupdict() if t in additional_regex else x

    def processor(match):
        return Match(
            time=int(match.group('TIME')),
            id=match.group('ID'),
            type=match.group('TYPE'),
            additional=additional_match(match.group('TYPE'), match.group('ADDITIONAL')),
        )

    return processor


def process_requests(sequence):
    times = []
    send_times = []    
    fails = 0
    backend_ok = defaultdict(Counter)
    backend_error = defaultdict(lambda: defaultdict(Counter))

    url_re = re.compile(r'http:\/\/(?P<NETLOC>[^\/]*).*')
    requests = defaultdict(dict) # buffer for storing request data
    for match in sequence:
        if match.type == 'StartRequest':
            requests[match.id]['start'] = match.time
            requests[match.id]['backends'] = defaultdict(dict)  

        if match.type == 'StartSendResult':
            requests[match.id]['send'] = match.time

        elif match.type.startswith('Backend'):
            gr = match.additional['GR']

            if match.type == 'BackendConnect':
                url_match = url_re.match(match.additional['URL'])
                requests[match.id]['backends'][gr] = url_match.group('NETLOC') if url_match else match.additional['URL'] 
                #urlparse(match.additional['URL']).netloc

            elif match.type == 'BackendError':
                url = requests[match.id]['backends'][gr]
                error = match.additional['ERROR']
                backend_error[gr][url][error] += 1

            elif match.type == 'BackendOk':
                url = requests[match.id]['backends'].pop(gr) # if request is ok, remove its url
                backend_ok[gr][url] += 1

        elif match.type == 'FinishRequest':           
            times.append(match.time - requests[match.id]['start'])
            send_times.append((match.id, match.time - requests[match.id]['send']))
            if requests[match.id]['backends']: # if some urls are left => error occured
                fails += 1
            del requests[match.id]
 
    return {
        'p95': times[int(len(times)*0.95)],
        'top10': map(lambda x: str(x[0]), heapq.nlargest(10, send_times, key=lambda x: x[1])),
        'fails': fails,
        'ok': backend_ok,
        'err': backend_error,
    }
