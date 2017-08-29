#!/usr/bin/env python

import sys
import os
import json
import logging

import requests
import pandas
import boto3
import synapseclient
import nda_aws_token_generator
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

# NDA Configuration
REFERENCE_GUID = 'NDAR_INVRT663MBL'

# This is an old genomics subject
EXCLUDE_GENOMICS_SUBJECTS = ('92027', )
# EXCLUDE_EXPERIMENTS = ('534', '535')
EXCLUDE_EXPERIMENTS = ()

NDA_BUCKET_NAME = 'nda-bsmn'

# Synapse configuration
# synapse_data_folder = 'syn7872188'
synapse_data_folder = 'syn10380539'
synapse_data_folder_id = int(synapse_data_folder.replace('syn', ''))
storage_location_id = '9209'

def main():

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry_run", action="store_true", default=False)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--get_manifests", action="store_true", default=False)

    args = parser.parse_args()

    # # Credential configuration for NDA
    s3 = boto3.resource("s3")
    obj = s3.Object('kdaily-lambda-creds.sagebase.org', 'ndalogs_config.json')

    config = json.loads(obj.get()['Body'].read())

    ndaconfig = config['nda']

    s3_nda = ndasynapse.nda.get_nda_s3_session(ndaconfig['username'], ndaconfig['password'])

    # Synapse
    # Using the concatenated manifests as the master list of files to store, create file handles and entities in Synapse.
    # Use the metadata table to get the appropriate tissue/subject/sample annotations to set on each File entity.

    auth = requests.auth.HTTPBasicAuth(ndaconfig['username'], ndaconfig['password'])
    bucket = s3_nda.Bucket(NDA_BUCKET_NAME)

    samples = ndasynapse.nda.get_samples(auth, guid=REFERENCE_GUID)

    # exclude some experiments
    samples = samples[~samples.experiment_id.isin(EXCLUDE_EXPERIMENTS)]
    samples = ndasynapse.nda.process_samples(samples)

    # samples.to_csv("./samples.csv")

    subjects = ndasynapse.nda.get_subjects(auth, REFERENCE_GUID)
    subjects = ndasynapse.nda.process_subjects(subjects, EXCLUDE_GENOMICS_SUBJECTS)

    btb = ndasynapse.nda.get_tissues(auth, REFERENCE_GUID)
    btb = ndasynapse.nda.process_tissues(btb)
    # btb.to_csv('./btb.csv')

    btb_subjects = ndasynapse.nda.merge_tissues_subjects(btb, subjects)

    metadata = ndasynapse.nda.merge_tissues_samples(btb_subjects, samples)

    if args.get_manifests:
        manifest = ndasynapse.nda.get_manifests(bucket)
        # Only keep the files that are in the metadata table
        manifest = manifest[manifest.filename.isin(metadata.data_file)]
        metadata_manifest = ndasynapse.nda.merge_metadata_manifest(metadata, manifest)
    else:
        metadata_manifest = metadata
        metadata_manifest = metadata_manifest.reindex(columns = metadata_manifest.columns.tolist() + ndasynapse.nda.METADATA_COLUMNS)

    # metadata_manifest.to_csv("/dev/stdout")

    # metadata_manifest = metadata_manifest.loc[0:5]

    logger.info("Finished creating manifest.")

    if not args.dry_run:
        syn = synapseclient.login(silent=True)
        fh_list = ndasynapse.synapse.create_synapse_filehandles(syn=syn,
                                                                metadata_manifest=metadata_manifest,
                                                                bucket_name=NDA_BUCKET_NAME,
                                                                storage_location_id=storage_location_id,
                                                                verbose=args.verbose)
        fh_ids = map(lambda x: x.get('id', None), fh_list)
        logger.debug(fh_ids)

        synapse_manifest = metadata_manifest[ndasynapse.nda.METADATA_COLUMNS]
        synapse_manifest['dataFileHandleId'] = fh_ids
        synapse_manifest['path'] = None

        fh_names = map(synapseclient.utils.guess_file_name, metadata_manifest.data_file.tolist())
        synapse_manifest['name'] = fh_names
        synapse_manifest['parentId'] = synapse_data_folder

        syn = synapseclient.login(silent=True)

        f_list = ndasynapse.synapse.store(syn=syn,
                                          synapse_manifest=synapse_manifest,
                                          filehandles=fh_list,
                                          dry_run=False)

        sys.stderr.write("%s\n" % (f_list, ))

if __name__ == "__main__":
    main()
