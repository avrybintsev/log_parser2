# -*- coding=utf-8 -*-
import re


def get_lines(filename):
    with open(filename, 'rU') as f:
        for line in f:
            yield line


def get_matches(reader, pattern, processor):
    regex = re.compile(pattern)
    
    for line in reader:
        match = regex.match(line)
        if match is not None:
            yield processor(match)
