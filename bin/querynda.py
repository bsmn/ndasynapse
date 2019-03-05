#!/usr/bin/env python

import os
import sys
import json
import logging
import uuid

import requests
import pandas
import boto3
import synapseclient
import ndasynapse

pandas.options.display.max_rows = None
pandas.options.display.max_columns = None
pandas.options.display.max_colwidth = 1000

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def get_submissions(auth, args):
    submissions = ndasynapse.nda.get_submissions(auth, collectionid=args.collection_id)
    submissions_processed = ndasynapse.nda.process_submissions(submissions)

    submissions_processed.to_csv(sys.stdout)

def main():

    import argparse
    import json
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--guids", type=str, default=REFERENCE_GUID, nargs="+",
                        help="GUID to search for. [default: %(default)s]")
    parser.add_argument("--config", type=str, default=None)

    subparsers = parser.add_subparsers(help='sub-command help')

    parser_get_submissions = subparsers.add_parser('get-submissions', help='Get submissions in NDA collections.')
    parser_get_submissions.add_argument('--collection_id', type=int, nargs="+", help='NDA collection IDs.')
    parser_get_submissions.set_defaults(func=get_submissions)

    args = parser.parse_args()
    
    config = json.loads(args.config)
    auth = ndasynapse.nda.authenticate(config)
    logger.info(auth)
    
    args.func(syn, args)


if __name__ == "__main__":
    main()
