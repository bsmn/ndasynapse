#!/usr/bin/env python3

"""
Program: manifest_guid_data.py

Purpose: Get the GUIDs for completed submissions for each BSMN NDA collection
         and create an appropriate summary data file for each manifest type.

Input parameters: File containing the user's NDA credentials (full path)
                  NDA manifest type (genomics_subject, genomics_sample,
                      nichd_btb)
                  Output file name
                  Optional Synapse ID of the table containing the collection
                      ID. The default is syn10802969 (Grant Data Summaries)
                  Optional column name containing the collection ID. The
                      default is "nda collection".

Outputs: csv file

Notes: - This script expects the user to have login credentials for Synapse.
       - We formerly would mine the manifests that were uploaded to the NDA
         for this information. However, we have discovered that when
         updated manifests are submitted to the NDA to correct inaccuracies
         in the original manifests, the corrections are applied to the
         database but the original manifest is not overwritten with the new
         one. We therefore have to query the database by GUID instead in order
         to get to the accurate information.

Execution: manifest_guid_data.py <NDA credentials file> 
               <NDA manifest type> <output file>
               --synapse_id <Synapse ID> --column_name <column name>
"""

import argparse
import io
import json
import logging
import pandas as pd
import requests
import synapseclient
import sys
sys.path.insert(0, "/home/cmolitor/bsmn_validation/develop_ndasynapse")
import ndasynapse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Synapse ID of the Grant Data Summaries table
COLLECTION_ID_LOCATION = "syn10802969"
COLLECTION_ID_COLUMN = "nda collection"

SUBJECT_MANIFEST = "genomics_subject"

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("nda_credentials", type=argparse.FileType('r'), 
                        help="File containing NDA user credentials (full path)")
    parser.add_argument("manifest_type", type=str,
                        help="NDA manifest type (genomics_sample03, genomics_subject02, nichd_btb02")
    parser.add_argument("out_file", type=argparse.FileType('w'), 
                        help="Output .csv file (full path)")
    parser.add_argument("--synapse_id", type=str, default=COLLECTION_ID_LOCATION,
                        help="Synapse ID for the entity containing the collection ID")
    parser.add_argument("--column_name", type=str, default=COLLECTION_ID_COLUMN,
                        help="Column containing the collection ID")

    args = parser.parse_args()

    guid_list = list()
    all_guids_df = pd.DataFrame()

    syn = synapseclient.Synapse()
    syn.login(silent=True)

    config = json.load(args.nda_credentials)

    # If the file containing the NDA credentials file has other sections,
    # grab the one for the 'nda' entry.
    if 'nda' in config.keys():
        config = config['nda']

    syn_table_query = f'SELECT distinct "{args.column_name}" from {args.synapse_id}'

    try:
        table_results_df = syn.tableQuery(syn_table_query).asDataFrame()
    except Exception as syn_query_error:
        raise syn_query_error

    # The link to the NDA collection will have a format similar to
    # https://ndar.nih.gov/edit_collection.html?id=<NDA collection ID>
    collection_id_list = (table_results_df[args.column_name].str.split("=", n=1).str[1]).tolist()
    logger.debug(collection_id_list)

    for coll_id in collection_id_list:

        # The NDASubmission class returns a list of dictionaries, with each dictionary
        # including the file content (['files']), the collection ID (['collection_id']),
        # and the submission ID (['submission_id']).

        submission_file_list = ndasynapse.nda.NDASubmission(config, collection_id=coll_id).submission_files
        for submission in submission_file_list:
            for data_file in submission["files"].data_files:
                data_file_as_string = data_file["content"].decode("utf-8")
                if SUBJECT_MANIFEST in data_file_as_string:
                    manifest_df = pd.read_csv(io.StringIO(data_file_as_string), skiprows=1)
                    for guid in manifest_df["subjectkey"].tolist():
                        guid_list.append(guid)

    # Get rid of any duplicates in the GUID list.
    guid_list = list(set(guid_list))

    # Cycle through the guids and query for the specified manifest type.
    for guid in guid_list:
        r = requests.get(f"https://nda.nih.gov/api/guid/{guid}/data?short_name={args.manifest_type}",
                         auth=requests.auth.HTTPBasicAuth(config["username"],
                                                          config["password"]),
                         headers={"Accept": "application/json"})

        guid_data = json.loads(r.text)
        
        # It is possible for there to be no data for the specified manifest type. If this
        # is the case, the GUID API will return an OK status (status_code = 200) and an
        # empty data structure, which will cause the code to crash further down, so check
        # to make sure that the data structure is not empty before continuing.
        if len(guid_data["age"]) == 0:
            continue

        # The documentation for the data structure is here:
        # https://nda.nih.gov/api/guid/docs/swagger-ui.html#!/guid/guidXMLTableUsingGET
        for ds_row in guid_data["age"][0]["dataStructureRow"]:
            manifest_data = dict()
            for de_row in ds_row["dataElement"]:
                manifest_data[de_row["name"]] = de_row["value"]

            # Get the collection number and add it to the manifest_data.
            for link_row in ds_row["links"]["link"]:
                if link_row["rel"].lower() == "collection":
                    manifest_data["collection_id"] = link_row["href"].split("=")[1]

            # Get the manifest data dictionary into a dataframe and flatten it out if necessary.
            manifest_flat_df = pd.io.json.json_normalize(manifest_data)
            all_guids_df = pd.concat([all_guids_df, manifest_flat_df], axis=0, ignore_index=True, sort=False)

    # Get rid of any rows that are exact duplicates except for the manifest ID column
    # (GENOMICS_SUBJECT02_ID, NICHD_BTB02_ID, GENOMICS_SAMPLE03_ID)
    manifest_id = (args.manifest_type + "_id").upper()
    all_guids_df.drop(manifest_id, axis=1, inplace=True)
    column_list = (all_guids_df.columns).tolist()
    pared_guids_df = all_guids_df.drop_duplicates(subset=column_list, keep="first")

    # Run the data through the list of BSMN collection IDs since it is possible for
    # the samples to have been used in other consortia.
    all_collections_df = pared_guids_df[pared_guids_df["collection_id"].isin(collection_id_list)]
    
    all_collections_df.to_csv(args.out_file, index=False)

    args.out_file.close()

if __name__ == "__main__":
    main()

# End of Program #
