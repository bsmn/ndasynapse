#!/usr/bin/env python

import sys
import logging

import pandas
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


def main():

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry_run", action="store_true", default=False)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--ignore_errors", action="store_true", default=False)
    parser.add_argument("--storage_location_id", type=str)
    parser.add_argument("--synapse_data_folder", type=str)
    parser.add_argument("manifest_file", type=str)

    args = parser.parse_args()

    syn = synapseclient.Synapse(skip_checks=True)
    syn.login(silent=True)

    # get existing storage location object
    storage_location = syn.restGET("/storageLocation/%(storage_location_id)s" % dict(storage_location_id=args.storage_location_id))

    metadata_manifest = pandas.read_csv(args.manifest_file)

    fh_list = ndasynapse.synapse.create_synapse_filehandles(syn=syn,
                                                            metadata_manifest=metadata_manifest,
                                                            storage_location=storage_location,
                                                            verbose=args.verbose)
    fh_ids = map(lambda x: x.get('id', None), fh_list)

    synapse_manifest = metadata_manifest
    synapse_manifest['dataFileHandleId'] = fh_ids
    synapse_manifest['path'] = None

    try:
        fh_names = metadata_manifest['fileName']
    except KeyError:
        logger.info("No column 'filename', using 'data_file' column.")
        fh_names = map(synapseclient.utils.guess_file_name,
                       metadata_manifest.data_file.tolist())

    synapse_manifest['name'] = fh_names

    synapse_manifest['parentId'] = args.synapse_data_folder

    if not args.dry_run:
        syn = synapseclient.login(silent=True)

        f_list = ndasynapse.synapse.store(syn=syn,
                                          synapse_manifest=synapse_manifest,
                                          filehandles=fh_list, ignore_errors=args.ignore_errors)

        sys.stderr.write("%s\n" % (f_list, ))
    else:
        synapse_manifest.to_csv("/dev/stdout", index=False, encoding='utf-8')


if __name__ == "__main__":
    main()
