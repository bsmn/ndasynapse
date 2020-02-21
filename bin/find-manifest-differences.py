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
import sys

import pandas as pd

import ndasynapse

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

PROCESS_REGISTRY = {'genomics_sample03': ndasynapse.nda.process_samples,
                    'genomics_subject02': ndasynapse.nda.process_subjects,
                    'nichd_btb02': ndasynapse.nda.process_tissues}

TO_DF_REGISTRY = {'genomics_sample03': ndasynapse.nda.sample_data_files_to_df,
                  'genomics_subject02': ndasynapse.nda.subjects_to_df,
                  'nichd_btb02': ndasynapse.nda.tissues_to_df}

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("guid_sample_data", type=argparse.FileType('r'),
                        help="Path to CSV file containing sample data from NDA GUID service.")
    parser.add_argument("collection_sample_data", type=argparse.FileType('r'),
                        help="Path to CSV file containing sample data from NDA collection submission service.")

    args = parser.parse_args()

    
    submission_genomics_sample = pd.read_csv(args.collection_sample_data)
    guid_genomics_sample = pd.read_csv(args.guid_sample_data)

    colnames_lower = [x.lower() for x in guid_genomics_sample.columns.tolist()]
    guid_genomics_sample.columns = colnames_lower
    guid_genomics_sample = guid_genomics_sample.astype(str)
    submission_genomics_sample = submission_genomics_sample.astype(str)

    _key_cols = ['subjectkey', 'experiment_id', 'src_subject_id', 'sample_id_original']    
    submission_genomics_sample = submission_genomics_sample.set_index(_key_cols)
    guid_genomics_sample = guid_genomics_sample.set_index(_key_cols)
    
    idx_difference = submission_genomics_sample.index.difference(guid_genomics_sample.index)
    
    diff_df = idx_difference.to_frame()
    
    diff_df = diff_df.reset_index(drop=True)
    # logger.debug(diff_df)
    if diff_df.shape[0] > 0:
        diff_df = diff_df.set_index(_key_cols)
        final_df = submission_genomics_sample.join(diff_df, how='left')
        final_df.to_csv(sys.stdout, index=False)

if __name__ == "__main__":
    main()
