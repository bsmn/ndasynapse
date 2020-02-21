#!/usr/bin/env python3
"""Get the GUIDs for completed submissions for each BSMN NDA collection and create an appropriate summary data file for each manifest type.

This script expects the user to have login credentials for Synapse.

We formerly would mine the manifests that were uploaded to the NDA for this information. 
However, we have discovered that when updated manifests are submitted to the NDA to 
correct inaccuracies in the original manifests, the corrections are applied to the
database but the original manifest is not overwritten with the new one. We therefore 
have to query the database by GUID instead in order to get to the accurate information.

Input parameters: 
    File containing the user's NDA credentials (full path)
    NDA manifest type (genomics_subject, genomics_sample,
        nichd_btb)
    Output file name
    Optional Synapse ID of the table containing the collection
        ID. The default is syn10802969 (Grant Data Summaries)
    Optional column name containing the collection ID. The
        default is "nda collection".

Output:
    csv-formatted data to standard out

Execution:
manifest_guid_data.py --config <NDA credentials file> 
    --synapse_id <Synapse ID> --column_name <column name>
    --manifest_type <NDA manifest type>

"""

import argparse
import json
import logging
import requests
import sys

import pandas as pd
import synapseclient

import ndasynapse

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Synapse ID of the Grant Data Summaries table
COLLECTION_ID_LOCATION = "syn10802969"
COLLECTION_ID_COLUMN = "nda_collection_id"

SUBJECT_MANIFEST = "genomics_subject"

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=None, 
                        help="Path to file containing NDA user credentials.")
    parser.add_argument("--synapse_id", type=str, default=COLLECTION_ID_LOCATION,
                        help="Synapse ID for the entity containing the collection ID")
    parser.add_argument("--column_name", type=str, default=COLLECTION_ID_COLUMN,
                        help="Column containing the collection ID")
    parser.add_argument("--manifest_type", type=str,
                        choices=["genomics_sample", "genomics_subject", "nichd_btb"],
                        help="NDA manifest type.")

    args = parser.parse_args()

    with open(args.config) as config_file:
        config = json.load(config_file)
        nda_config = config['nda']

    auth = ndasynapse.nda.authenticate(config)

    syn = synapseclient.login(silent=True)

    syn_table_query = f'SELECT distinct "{args.column_name}" from {args.synapse_id}'
    table_results_df = syn.tableQuery(syn_table_query).asDataFrame()

    collection_id_list = table_results_df[args.column_name].tolist()

    logger.debug(collection_id_list)

    all_data = []

    for coll_id in collection_id_list:
        nda_collection = ndasynapse.nda.NDACollection(nda_config, collection_id=coll_id)

        for submission in nda_collection.submissions:
            
            try:
                ndafiles = submission.submission_files['files']
            except IndexError:
                logger.info(f"No submission files for collection {coll_id}.")
                continue

            manifest_data = ndafiles.manifest_to_df(args.manifest_type)

            if manifest_data is not None and manifest_data.shape[0] > 0:
                manifest_data['collection_id'] = nda_collection.collection_id
                manifest_data['submission_id'] = submission.submission_id
                all_data.append(manifest_data)
            else:
                logger.info(f"No {args.manifest_type} data found for submission {submission.submission_id}.")

    all_data_df = pd.concat(all_data, axis=0, ignore_index=True, sort=False)
    all_data_df.to_csv(sys.stdout, mode='a', index=False)

if __name__ == "__main__":
    main()
