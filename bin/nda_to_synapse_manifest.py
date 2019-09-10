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

logging.basicConfig()
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

NDA_BUCKET_NAME = 'nda-bsmn'

# Synapse configuration
storage_location_id = '9209'
PROJECT_ID = 'syn5902559'

def main():

    import argparse
    import json
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--guids", type=str, default=REFERENCE_GUID, nargs="+",
                        help="GUID to search for. [default: %(default)s]")
    parser.add_argument("--get_experiments", action="store_true", default=False)
    parser.add_argument("--dataset_ids", default=None, nargs="*")
    parser.add_argument("--config", type=str, default=None)

    args = parser.parse_args()

    config = json.load(open(args.config))
    auth = ndasynapse.nda.authenticate(config)
    logger.info(auth)
    
    # Synapse
    # Using the concatenated manifests as the master list of files to store, create file handles and entities in Synapse.
    # Use the metadata table to get the appropriate tissue/subject/sample annotations to set on each File entity.

    samples = pandas.DataFrame()
    subjects = pandas.DataFrame()
    btb = pandas.DataFrame()
    
    for guid in args.guids:
        samples_guid = ndasynapse.nda.get_samples(auth, guid=guid)
        logger.debug(f"Got {len(samples_guid)} samples for {guid}")
        samples_guid = ndasynapse.nda.sample_data_files_to_df(samples_guid)

        # exclude some experiments
        samples_guid = ndasynapse.nda.process_samples(samples_guid)

        # TEMPORARY FIXES - NEED TO BE ADJUSTED AT NDA
        try:
            logger.debug("Fixing Salk site samples still. Check with NDA to confirm change.")
            samples_guid.loc[samples_guid['site'] == 'Salk', 'site'] = 'U01MH106882'
        except KeyError:
            pass
        
        subjects_guid = ndasynapse.nda.get_subjects(auth, guid)
        subjects_guid = ndasynapse.nda.subjects_to_df(subjects_guid)
        subjects_guid = ndasynapse.nda.process_subjects(subjects_guid,
                                                        EXCLUDE_GENOMICS_SUBJECTS)
        
        btb_guid = ndasynapse.nda.get_tissues(auth, guid)
        btb_guid = ndasynapse.nda.tissues_to_df(btb_guid)
        btb_guid = ndasynapse.nda.process_tissues(btb_guid)

        samples = samples.append(samples_guid)
        subjects = subjects.append(subjects_guid)
        btb = btb.append(btb_guid)

    btb_subjects = ndasynapse.nda.merge_tissues_subjects(btb, subjects)    
    metadata = ndasynapse.nda.merge_tissues_samples(btb_subjects, samples)

    if args.dataset_ids:
        metadata = metadata[metadata.datasetid.isin(args.dataset_ids)]
        logger.info("Filtered for requested dataset IDs, %s records remaining." % metadata.shape[0])

    if args.get_experiments:
        if args.verbose:
            logger.info("Getting experiments")

        experiment_ids = metadata.experiment_id.drop_duplicates().tolist()
        logger.info("Experiments to get: %s" % (experiment_ids,))

        if experiment_ids:
            expts = ndasynapse.nda.get_experiments(auth,
                                                   experiment_ids,
                                                   verbose=args.verbose)

            expts = ndasynapse.nda.process_experiments(expts)
            expts = expts.drop_duplicates()

            logger.info(f"{expts.shape[0]} experiments found.")
            metadata = metadata.merge(expts, how="left", left_on="experiment_id",
                                      right_on="experiment_id")
            logger.info("Retrieved experiments.")
        else:
            logger.info("No experiments retrieved")
    
    # Look for duplicates based on base filename
    # We are putting all files into a single folder, so can't conflict on name
    # Decided to rename both the entity name and the downloadAs
    metadata['basename'] = metadata.data_file.apply(os.path.basename)

    (good, bad) = ndasynapse.nda.find_duplicate_filenames(metadata)

    if bad.shape[0] > 0:
        syn = synapseclient.login(silent=True)

        try:
            namespace = uuid.UUID(ndasynapse.synapse.get_namespace(syn,
                                                                   PROJECT_ID))
            
            bad_uuids = bad.data_file.apply(lambda x: uuid.uuid3(namespace,
                                                                 x))
            bad_slugs = bad_uuids.apply(lambda x: ndasynapse.synapse.uuid2slug(x))
            bad_slugs.name = 'slug'
            
            bad_filename_info = pandas.concat([bad_slugs, bad['basename']], axis=1)
            fileNameOverride = bad_filename_info.apply(lambda x: '_'.join(map(str, x)), axis=1)
            
            bad['fileName'] = fileNameOverride
            good['fileName'] = good['basename']
            
            metadata = pandas.concat([good, bad])
        except KeyError:
            logging.info("Couldn't get namespace. Not processing bad files")
            metadata = good
    else:
        metadata = good

    metadata.drop('basename', inplace=True, axis=1)

    metadata['consortium'] = "BSMN"

    logger.info("Writing manifest.")

    metadata.to_csv("/dev/stdout", index=False, encoding='utf-8')


if __name__ == "__main__":
    main()
