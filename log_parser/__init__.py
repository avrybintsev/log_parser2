# -*- coding=utf-8 -*-
from parser import get_lines, get_matches
from output import output
from process import get_processor, process_requests, PATTERN, ADDITIONAL_PATTERNS

def log_parser(in_file, out_file):
    output(
        out_file, 
        process_requests(
            get_matches(
                reader=get_lines(in_file), 
                pattern=PATTERN, 
                processor=get_processor(additional_patterns=ADDITIONAL_PATTERNS)
            )
        )
    )
