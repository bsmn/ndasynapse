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
import csv
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

def get_collection_ids_from_links(data_structure_row: dict) -> set:
    """Get a set of collection IDs from a data structure row from the NDA GUID API.

    Args:
        data_structure_row: a dictionary from the JSON returned by the NDA GUID data API.
    Returns:
        a set of collection IDs as integers.

    """

    curr_collection_ids = set()
    for link_row in data_structure_row["links"]["link"]:
        if link_row["rel"].lower() == "collection":
            curr_collection_ids.add(int(link_row["href"].split("=")[1]))
    
    if len(curr_collection_ids) > 1:
        logger.warn(f"Found different collection ids: {curr_collection_ids}")

    return curr_collection_ids

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=None, 
                        help="Path to file containing NDA user credentials.")
    parser.add_argument('--collection_id', type=int, nargs="+", help='NDA collection IDs.')
    parser.add_argument("--manifest_type", type=str,
                        choices=["genomics_sample03", "genomics_subject02", "nichd_btb02"],
                        help="NDA manifest type.")

    args = parser.parse_args()

    guid_list = list()
    all_guids_df = pd.DataFrame()

    syn = synapseclient.login(silent=True)

    with open(args.config) as config_file:
        config = json.load(config_file)
        nda_config = config['nda']

    auth = ndasynapse.nda.authenticate(config)

    collection_id_list = args.collection_id
        
    logger.debug(collection_id_list)

    for coll_id in collection_id_list:

        # The NDASubmission class returns a list of dictionaries, with each dictionary
        # including the file content (['files']), the collection ID (['collection_id']),
        # and the submission ID (['submission_id']).

        nda_collection = ndasynapse.nda.NDACollection(nda_config, collection_id=coll_id)
                
        # Cycle through the guids and query for the specified manifest type.
        for guid in nda_collection.guids:
            
            guid_data = ndasynapse.nda.get_guid_data(auth=auth, subjectkey=guid, 
                                                    short_name=args.manifest_type)
            
            # It is possible for there to be no data for the specified manifest type. If this
            # is the case, the GUID API will return an OK status (status_code = 200) and an
            # empty data structure, which will cause the code to crash further down, so check
            # to make sure that the data structure is not empty before continuing.
            if guid_data is None or len(guid_data["age"]) == 0:
                logger.debug(f"No data for guid {guid}")
                continue

            # The documentation for the data structure is here:
            # https://nda.nih.gov/api/guid/docs/swagger-ui.html#!/guid/guidXMLTableUsingGET
            for ds_row in guid_data["age"][0]["dataStructureRow"]:

                curr_collection_ids = get_collection_ids_from_links(data_structure_row=ds_row)

                # If the current collection ID we're interested in isn't in the current ids
                # then we should keep going - this data is not relevant now!
                if coll_id not in curr_collection_ids:
                    continue
                
                manifest_data = dict()

                # Add the collection number to the manifest_data.
                manifest_data["collection_id"] = coll_id

                # Get all of the metadata
                for de_row in ds_row["dataElement"]:
                    manifest_data[de_row["name"]] = de_row["value"]

                # Get the manifest data dictionary into a dataframe and flatten it out if necessary.
                manifest_flat_df = pd.io.json.json_normalize(manifest_data)
                all_guids_df = pd.concat([all_guids_df, manifest_flat_df], 
                                         axis=0, ignore_index=True, sort=False)

                # Get rid of any rows that are exact duplicates except for the manifest ID column
                # (GENOMICS_SUBJECT02_ID, NICHD_BTB02_ID, GENOMICS_SAMPLE03_ID)
                manifest_id = (args.manifest_type + "_id").upper()
                all_guids_df.drop(manifest_id, axis=1, inplace=True)
                column_list = (all_guids_df.columns).tolist()
                all_guids_df = all_guids_df.drop_duplicates(subset=column_list, keep="first")

    # Run the data through the list of BSMN collection IDs since it is possible for
    # the samples to have been used in other consortia.
    # TODO (KD): think this can be removed with the new checks above.
    all_collections_df = all_guids_df # [all_guids_df["collection_id"].isin(collection_id_list)]
    
    all_collections_df.to_csv(sys.stdout, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
    main()
