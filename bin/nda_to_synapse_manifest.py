#!/usr/bin/env python

import os
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

# NDA Configuration
REFERENCE_GUID = 'NDAR_INVRT663MBL'

# This is an old genomics subject
EXCLUDE_GENOMICS_SUBJECTS = ('92027', )
# EXCLUDE_EXPERIMENTS = ('534', '535')
EXCLUDE_EXPERIMENTS = ()

NDA_BUCKET_NAME = 'nda-bsmn'

# Synapse configuration
storage_location_id = '9209'
PROJECT_ID = 'syn5902559'
UUID_COLUMNS = ['sample_id_biorepository', 'sample_id_original',
                'experiment_id', 'datasetid']


def main():

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--get_experiments", action="store_true", default=False)
    parser.add_argument("--synapse_data_folder", nargs=1)
    parser.add_argument("--uuid_columns", type=str, default=None)
    parser.add_argument("--dataset_ids", default=None, nargs="*")

    args = parser.parse_args()

    # # Credential configuration for NDA
    s3 = boto3.resource("s3")
    obj = s3.Object('kdaily-lambda-creds.sagebase.org', 'ndalogs_config.json')

    config = json.loads(obj.get()['Body'].read())

    ndaconfig = config['nda']

    # Synapse
    # Using the concatenated manifests as the master list of files to store, create file handles and entities in Synapse.
    # Use the metadata table to get the appropriate tissue/subject/sample annotations to set on each File entity.

    auth = requests.auth.HTTPBasicAuth(ndaconfig['username'], ndaconfig['password'])

    samples = ndasynapse.nda.get_samples(auth, guid=REFERENCE_GUID)

    # exclude some experiments
    samples = samples[~samples.experiment_id.isin(EXCLUDE_EXPERIMENTS)]
    samples = ndasynapse.nda.process_samples(samples)

    # TEMPORARY FIXES - NEED TO BE ADJUSTED AT NDA
    change_grant_ids = ['741', '743', '744', '745', '746']
    samples.loc[samples['experiment_id'].isin(change_grant_ids), 'site'] = 'U01MH106892'
    samples.loc[samples['site'] == 'Salk', 'site'] = 'U01MH106882'

    subjects = ndasynapse.nda.get_subjects(auth, REFERENCE_GUID)
    subjects = ndasynapse.nda.process_subjects(subjects,
                                               EXCLUDE_GENOMICS_SUBJECTS)

    btb = ndasynapse.nda.get_tissues(auth, REFERENCE_GUID)
    btb = ndasynapse.nda.process_tissues(btb)

    btb_subjects = ndasynapse.nda.merge_tissues_subjects(btb, subjects)

    metadata = ndasynapse.nda.merge_tissues_samples(btb_subjects, samples)

    if args.dataset_ids:
        metadata = metadata[metadata.datasetid.isin(args.dataset_ids)]
        logger.info("Filtered for requested dataset IDs.")

    if args.get_experiments:
        if args.verbose:
            logger.info("Getting experiments")

        experiment_ids = metadata.experiment_id.drop_duplicates().tolist()
        expts = ndasynapse.nda.get_experiments(auth,
                                               experiment_ids,
                                               verbose=args.verbose)

        expts = ndasynapse.nda.process_experiments(expts)

        metadata = metadata.merge(expts, how="left", left_on="experiment_id",
                                  right_on="experiment_id")
        logger.info("Retrieved experiments.")

    # Look for duplicates based on base filename
    # We are putting all files into a single folder, so can't conflict on name
    # Decided to rename both the entity name and the downloadAs
    metadata.loc[:, 'basename'] = metadata.data_file.apply(lambda x: os.path.basename(x))

    (good, bad) = ndasynapse.nda.find_duplicate_filenames(metadata)

    if bad.shape[0] > 0:
        syn = synapseclient.login(silent=True)

        namespace = uuid.UUID(ndasynapse.synapse.get_namespace(syn, PROJECT_ID))
        bad_uuids = bad.data_file.apply(lambda x: uuid.uuid3(namespace,
                                                             x))
        bad_slugs = bad_uuids.apply(lambda x: ndasynapse.synapse.uuid2slug(x))
        bad_slugs.name = 'slug'

        bad_filename_info = pandas.concat([bad_slugs, bad['basename']], axis=1)
        fileNameOverride = bad_filename_info.apply(lambda x: '_'.join(map(str, x)), axis=1)

        bad['fileName'] = fileNameOverride
        good['fileName'] = good['basename']

        metadata = pandas.concat([good, bad])
    else:
        metadata = good

    metadata.drop('basename', inplace=True, axis=1)

    metadata['consortium'] = "BSMN"

    logger.info("Writing manifest.")

    metadata.to_csv("/dev/stdout", index=False, encoding='utf-8')


if __name__ == "__main__":
    main()
