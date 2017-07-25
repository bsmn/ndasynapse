import os
import json
import synapseclient
import pandas
import requests
import boto3
import nda_aws_token_generator
import logging

pandas.options.display.max_rows = None
pandas.options.display.max_columns = None
pandas.options.display.max_colwidth = 1000

logger = logging.getLogger("main")
logger.setLevel(logging.DEBUG)
#create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# NDA Configuration
REFERENCE_GUID = 'NDAR_INVRT663MBL'

# Synapse configuration
import synapseclient
synapse_data_folder = 'syn7872188'
synapse_data_folder_id = int(synapse_data_folder.replace('syn', ''))
storage_location_id = '9209'

content_type_dict = {'.gz': 'application/x-gzip', '.bam': 'application/octet-stream',
                     '.zip': 'application/zip'}

syn = synapseclient.login(silent=True)
dry_run = False

def create_synapse_filehandles(metadata_manifest, bucket_name):
    """Create a list of Synapse file handles (S3FileHandles) to link to."""

    fh_list = []

    for n, x in metadata_manifest.iterrows():
        s3Key = x['filename'].replace("s3://%s/" % bucket_name, "")
        s3FilePath = os.path.split(s3Key)[-1]
        contentSize = x['size']
        contentMd5 = x['md5']

        logger.debug("%s - %s" % (s3Key, s3FilePath))

        # Check if it exists in Synapse
        res = syn.restGET("/entity/md5/%s" % (contentMd5, ))['results']

        res = filter(lambda x: x['benefactorId'] == synapse_data_folder_id, res)

        if len(res) > 0:
            fhs = [syn.restGET("/entity/%(id)s/version/%(versionNumber)s/filehandles" % er) for er in res]
            fileHandleObj = syn._getFileHandle(fhs[0]['list'][0]['id'])
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

            fileHandleObj = syn.restPOST('/externalFileHandle/s3',
                                         json.dumps(fileHandle),
                                         endpoint=syn.fileHandleEndpoint)

        fh_list.append(fileHandleObj)

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
