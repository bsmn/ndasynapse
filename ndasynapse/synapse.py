import os
import json
import requests
import logging

import pandas
import boto3
import synapseclient
import nda_aws_token_generator

pandas.options.display.max_rows = None
pandas.options.display.max_columns = None
pandas.options.display.max_colwidth = 1000

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Synapse configuration
import synapseclient
synapse_data_folder = 'syn7872188'
synapse_data_folder_id = int(synapse_data_folder.replace('syn', ''))

dry_run = False

def create_synapse_filehandles(syn, metadata_manifest, bucket_name, storage_location_id):
    """Create a list of Synapse file handles (S3FileHandles) to link to."""

    fh_list = []

    for n, x in metadata_manifest.iterrows():
        s3Key = x['filename'].replace("s3://%s/" % bucket_name, "")
        s3FilePath = os.path.split(s3Key)[-1]
        contentSize = x['size']
        contentMd5 = x['md5']

        # Check if it exists in Synapse
        res = syn.restGET("/entity/md5/%s" % (contentMd5, ))['results']

        res = filter(lambda x: x['benefactorId'] == synapse_data_folder_id, res)

        if len(res) > 0:
            fhs = [syn.restGET("/entity/%(id)s/version/%(versionNumber)s/filehandles" % er) for er in res]
            fileHandle = syn._getFileHandle(fhs[0]['list'][0]['id'])
        else:
            contentType = content_type_dict.get(os.path.splitext(x['filename'])[-1],
                                                'application/octet-stream')

            fileHandle = {'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
            'fileName'    : s3FilePath,
            'contentSize' : contentSize,
            'contentType' : contentType,
            'contentMd5' :  contentMd5,
            'bucketName' : NDA_BUCKET_NAME,
            'key'        : s3Key,
            'storageLocationId' : storage_location_id}

            logger.debug("Doesn't exist: %s - %s" % (s3Key, s3FilePath))


            # fileHandle = syn.restPOST('/externalFileHandle/s3',
            #                           json.dumps(fileHandle),
            #                           endpoint=syn.fileHandleEndpoint)

        fh_list.append(fileHandle)

    return fh_list

def store(synapse_manifest):

    f_list = []

    for x in synapse_manifest.iterrows():
        logger.debug("filename = %s, annotations = %s" % (x['filename'], a))

        if not dry_run:
            f = synapseclient.File(parentId=synapse_data_folder,
                                   name=s3FilePath,
                                   dataFileHandleId = fileHandleObj['id'])
            f.annotations = a

            f = syn.store(f, forceVersion=False)

            f_list.append(f)

    return f_list
