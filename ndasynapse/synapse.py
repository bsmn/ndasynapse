import os
import json
import logging

import requests
import pandas
import boto3
import synapseclient
import nda_aws_token_generator

pandas.options.display.max_rows = None
pandas.options.display.max_columns = None
pandas.options.display.max_colwidth = 1000

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# logger.addHandler(ch)

# Synapse configuration
import synapseclient

dry_run = False

content_type_dict = {'.gz': 'application/x-gzip',
                     '.bam': 'application/octet-stream',
                     '.zip': 'application/zip'}

def create_synapse_filehandles(syn, metadata_manifest, bucket_name, storage_location_id, verbose=False):
    """Create a list of Synapse file handles (S3FileHandles) to link to."""

    fh_list = []

    for n, x in metadata_manifest.iterrows():
        s3Key = x['data_file'].replace("s3://%s/" % bucket_name, "")
        s3FilePath = os.path.split(s3Key)[-1]
        contentSize = x['size']
        contentMd5 = x['md5']

        # Check if it exists in Synapse
        res = syn.restGET("/entity/md5/%s" % (contentMd5, ))['results']

        if verbose:
            logger.debug("Checked for md5 %s" % contentMd5)

        # res = filter(lambda x: x['benefactorId'] == synapse_data_folder_id, res)

        if len(res) > 0:
            fhs = [syn.restGET("/entity/%(id)s/version/%(versionNumber)s/filehandles" % er) for er in res]
            fileHandle = syn._getFileHandle(fhs[0]['list'][0]['id'])

            if verbose:
                logger.debug("Got filehandle for %s" % fhs[0]['list'][0]['id'])

        else:
            contentType = content_type_dict.get(os.path.splitext(x['data_file'])[-1],
                                                'application/octet-stream')

            fileHandle = {'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
                          'fileName': s3FilePath,
                          'contentSize': contentSize,
                          'contentType': contentType,
                          'contentMd5': contentMd5,
                          'bucketName': bucket_name,
                          'key': s3Key,
                          'storageLocationId': storage_location_id}

            logger.debug("Doesn't exist: %s - %s" % (s3Key, s3FilePath))

            # fileHandle = syn.restPOST('/externalFileHandle/s3',
            #                          json.dumps(fileHandle),
            #                          endpoint=syn.fileHandleEndpoint)

        fh_list.append(fileHandle)

    return fh_list

def store(syn, synapse_manifest, filehandles, dry_run=False):

    f_list = []

    for (row, file_handle) in zip(synapse_manifest.iterrows(), filehandles):

        i, x = row
        a = x.to_dict()

        if not dry_run:

            if not file_handle.get('id'):
                stored_file_handle = syn.restPOST('/externalFileHandle/s3',
                                                  json.dumps(file_handle),
                                                  endpoint=syn.fileHandleEndpoint)
                x.dataFileHandleId = stored_file_handle['id']
            else:
                stored_file_handle = file_handle
                if stored_file_handle['id'] != x.dataFileHandleId:
                    raise ValueError("Not equal: %s != %s" % (stored_file_handle['id'],
                                                              x.dataFileHandleId))

            f = synapseclient.File(**a)
            f = syn.store(f, forceVersion=False)

            f_list.append(f)

    return f_list
