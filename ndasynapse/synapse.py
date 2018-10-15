import os
import json
import uuid
import logging

import pandas
import synapseclient

pandas.options.display.max_rows = None
pandas.options.display.max_columns = None
pandas.options.display.max_colwidth = 1000

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# logger.addHandler(ch)

# Synapse configuration
dry_run = False

content_type_dict = {'.gz': 'application/x-gzip',
                     '.bam': 'application/octet-stream',
                     '.zip': 'application/zip'}


def check_existing_by_datasetid(syn, datasetids, file_view_id):
    """Check a file view that has a 'datasetid' column to see which datasetids exist.

    """

    res = syn.tableQuery('select id,datasetid from %s' % (file_view_id, ))
    d = res.asDataFrame()

    existing_datasetids = set(d.datasetid.tolist())
    given_datasetids = set(datasetids)

    return {'exists': given_datasetids.intersection(existing_datasetids),
            'not_exists': given_datasetids.difference(existing_datasetids)}


def create_synapse_filehandles(syn, metadata_manifest, storage_location, verbose=False):
    """Create a list of Synapse file handles (S3FileHandles) to link to."""

    fh_list = []

    for n, x in metadata_manifest.iterrows():
        s3Key = x['data_file'].replace("s3://%(bucket)s/" % storage_location, "")

        try:
            file_path = x['fileName']
        except KeyError:
            file_path = os.path.split(s3Key)[-1]

        contentSize = x['size']
        contentMd5 = x['md5']

        # Check if it exists in Synapse
        res = syn.restGET("/entity/md5/%s" % (contentMd5, ))['results']

        if verbose:
            logger.info("Checked for md5 %s" % contentMd5)

        # res = filter(lambda x: x['benefactorId'] == synapse_data_folder_id, res)

        if len(res) > 0:
            fhs = [syn.restGET("/entity/%(id)s/version/%(versionNumber)s/filehandles" % er) for er in res]
            fileHandle = syn._getFileHandle(fhs[0]['list'][0]['id'])

            if verbose:
                logger.info("Got filehandle for %s" % fhs[0]['list'][0]['id'])

        else:
            contentType = content_type_dict.get(os.path.splitext(x['data_file'])[-1],
                                                'application/octet-stream')

            fileHandle = {'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
                          'fileName': file_path,
                          'contentSize': contentSize,
                          'contentType': contentType,
                          'contentMd5': contentMd5,
                          'bucketName': storage_location['bucket'],
                          'key': s3Key,
                          'storageLocationId': storage_location['storageLocationId']}

            if verbose:
                logger.info("Doesn't exist: %s - %s" % (s3Key, file_path))

        fh_list.append(fileHandle)

    return fh_list

def get_filehandles_by_md5(syn, md5):
    res = syn.restGET("/entity/md5/%s" % md5)

    fhs = [syn.restGET("/entity/%(id)s/version/%(versionNumber)s/filehandles" % er) for er in res]

    return fhs


def entity_by_md5(syn, contentMd5, parentId=None, cmp=None):
    """Gets the first entity in a list of entities identified by md5.

    Optionally takes a comparison function to pass to sorted
    and a parent id for filtering.

    """

    # Check if it exists in Synapse
    res = syn.restGET("/entity/md5/%s" % (contentMd5, ))['results']

    if cmp:
        res = sorted(res, cmp=cmp)

    if parentId:
        res = filter(lambda x: x['parentId'] == parentId, res)

    try:
        entity = syn.get(res[0]['id'], version=res[0]['versionNumber'])
    except KeyError:
        entity = None

    return entity


def get_namespace(syn, projectId):
    return syn.getAnnotations(projectId)['namespace_uuid'][0]


def uuid2slug(uuid):
    return uuid.bytes.encode('base64').rstrip('=\n').replace('/', '_')


def slug2uuid(slug):
    my_slug = uuid.UUID(bytes=(slug + '==').replace('_', '/').decode('base64'))
    return str(my_slug)


def store(syn, synapse_manifest, filehandles, verbose=False):

    f_list = []

    for (row, file_handle) in zip(synapse_manifest.iterrows(), filehandles):

        i, x = row
        a = x.to_dict()

        if not file_handle.get('id'):
            stored_file_handle = syn.restPOST('/externalFileHandle/s3',
                                              json.dumps(file_handle),
                                              endpoint=syn.fileHandleEndpoint)
            a['dataFileHandleId'] = stored_file_handle['id']
        else:
            if file_handle['id'] != a['dataFileHandleId']:
                raise ValueError("Not equal: %s != %s" % (file_handle['id'],
                                                          a['dataFileHandleId']))

        f = synapseclient.File(**a)
        f = syn.store(f, forceVersion=False)

        if verbose:
            logger.debug("Stored %s (%s) to parentId %s" % (row.name, f.id, row.parentId))

        f_list.append(f)

    return f_list
