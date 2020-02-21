"""Functions to interact with NIMH Data Archive API.

"""

import io
import os
import json
import logging
import sys

import requests
import pandas
import boto3
from deprecated import deprecated

pandas.options.display.max_rows = None
pandas.options.display.max_columns = None
pandas.options.display.max_colwidth = 1000

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

METADATA_COLUMNS = ['src_subject_id', 'experiment_id', 'subjectkey',
                    'sample_id_original', 'sample_id_biorepository',
                    'subject_sample_id_original', 'biorepository',
                    'subject_biorepository', 'sample_description',
                    'species', 'site', 'sex', 'sample_amount', 'phenotype',
                    'comments_misc', 'sample_unit', 'fileFormat']

SAMPLE_COLUMNS = ['collection_id', 'datasetid', 'experiment_id',
                  'sample_id_original', 'storage_protocol',
                  'sample_id_biorepository', 'organism', 'sample_amount', 'sample_unit',
                  'biorepository', 'comments_misc', 'site', 'genomics_sample03_id',
                  'src_subject_id', 'subjectkey']

SUBJECT_COLUMNS = ['src_subject_id', 'subjectkey', 'sex', 'race', 'phenotype',
                   'subject_sample_id_original', 'sample_description',
                   'subject_biorepository', 'sex']

EXPERIMENT_COLUMNS_CHANGE = {'additionalinformation.analysisSoftware.software': 'analysisSoftwareName',
                             'additionalinformation.equipment.equipmentName': 'equipmentName',
                             'experimentparameters.molecule.moleculeName': 'moleculeName',
                             'experimentparameters.platform.platformName': 'platformName',
                             'experimentparameters.platform.platformSubType': 'platformSubType',
                             'experimentparameters.platform.vendorName': 'vendorName',
                             'experimentparameters.technology.applicationName': 'applicationName',
                             'experimentparameters.technology.applicationSubType': 'applicationSubType',
                             'extraction.extractionProtocols.protocolName': 'extractionProtocolName',
                             'extraction.extractionKits.extractionKit': 'extractionKit',
                             'processing.processingKits.processingKit': 'processingKit'}

EQUIPMENT_NAME_REPLACEMENTS = {'Illumina HiSeq 2500,Illumina NextSeq 500': 'HiSeq2500,NextSeq500',
                               'Illumina NextSeq 500,Illumina HiSeq 2500': 'HiSeq2500,NextSeq500',
                               'Illumina HiSeq 4000,Illumina MiSeq': 'HiSeq4000,MiSeq',
                               'Illumina MiSeq,Illumina HiSeq 4000': 'HiSeq4000,MiSeq',
                               'Illumina NextSeq 500': 'NextSeq500',
                               'Illumina HiSeq 2500': 'HiSeq2500',
                               'Illumina HiSeq X Ten': 'HiSeqX',
                               'Illumina HiSeq 4000': 'HiSeq4000',
                               'Illumina MiSeq': 'MiSeq',
                               'BioNano IrysView': 'BionanoIrys'}

APPLICATION_SUBTYPE_REPLACEMENTS = {"Whole genome sequencing": "wholeGenomeSeq",
                                    "Exome sequencing": "exomeSeq",
                                    "Optical genome imaging": "wholeGenomeOpticalImaging"}

MANIFEST_COLUMNS = ['filename', 'md5', 'size']

def authenticate(config):
    """Authenticate to NDA.

    Args:
        config: A dict with 'username' and 'password' keys for NDA login.
    Returns:
        A requests.auth.HTTPBasicAuth object.

    """
    try:
        ndaconfig = config['nda']
    except KeyError:
        raise KeyError("Cannot find NDA credentials in config file.")

    auth = requests.auth.HTTPBasicAuth(ndaconfig['username'],
                                       ndaconfig['password'])

    return auth


def get_guid(auth, subjectkey: str) -> dict:
    """Get available data from the GUID API.

    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        subjectkey: An NDA GUID (Globally Unique Identifier)
    Returns:
        dict from JSON format.
    """

    req = requests.get(f"https://nda.nih.gov/api/guid/{subjectkey}/",
                       auth=auth, headers={'Accept': 'application/json'})

    logger.debug(f"Request {req} for GUID {subjectkey}")

    if req.ok:
        return req.json()
    else:
        logger.debug(f"{req.status_code} - {req.url} - {req.text}")
        return None


def get_guid_data(auth, subjectkey: str, short_name: str) -> dict:
    """Get data from the GUID API.

    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        subjectkey: An NDA GUID (Globally Unique Identifier)
        short_name: The data structure to return data for
                    (e.g., genomics_sample03)
    Returns:
        dict from JSON format.
    """

    req = requests.get(f"https://nda.nih.gov/api/guid/{subjectkey}/data?short_name={short_name}",  # pylint: disable=line-too-long
                       auth=auth, headers={'Accept': 'application/json'})

    logger.debug(f"Request {req} for GUID {subjectkey}")

    if req.ok:
        return req.json()
    else:
        logger.debug(f"{req.status_code} - {req.url} - {req.text}")
        return None


def get_samples(auth, guid: str) -> dict:
    """Use the NDA api to get the `genomics_sample03` records for a GUID.

    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        guid: An NDA GUID (Globally Unique Identifier)
    Returns:
        dict from JSON format.
    """

    return get_guid_data(auth=auth, subjectkey=guid,
                         short_name="genomics_sample03")


def get_subjects(auth, guid):
    """Use the NDA API to get the `genomics_subject02` records for a GUID.

        Args:
            auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
            guid: An NDA GUID (also called the subjectkey).
        Returns:
            Data in JSON format.
    """

    return get_guid_data(auth=auth, subjectkey=guid, short_name="genomics_subject02")

def get_tissues(auth, guid):
    """Use the NDA GUID API to get the `ncihd_btb02` records for a GUID.

    These records are the brain and tissue bank information.

    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        guid: An NDA GUID (also called the subjectkey).
    Returns:
        Data in JSON format.

    """

    return get_guid_data(auth=auth, subjectkey=guid, short_name="nichd_btb02")


def get_submission(auth, submissionid: int) -> dict:
    """Use the NDA Submission API to get a submission.

    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        guid: An NDA submission ID.
    Returns:
        dict from JSON format.
    """

    req = requests.get(f"https://nda.nih.gov/api/submission/{submissionid}",
                       auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for submission %s" % (req, submissionid))

    if req.ok:
        return req.json()
    else:
        logger.debug(f"{req.status_code} - {req.url} - {req.text}")
        return None


def get_submissions(auth, collectionid, status="Upload Completed", users_own_submissions=False):
    """Use the NDA Submission API to get submissions from a NDA collection.

    This is a separate service to get submission in batch that are related to
    a collection or a user.

    See `get_submission` to get a single submission by submission ID.

    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        collectionid: An NDA collection ID or a list of NDA collection IDs.
                      If None, gets all submissions.
        status: Status of submissions to retrieve. If None, gets
                all submissions.
        users_own_submissions: Return only user's own submissions.
                               If False, must pass collection ID(s).
    Returns:
        dict from JSON format.

    """

    if isinstance(collectionid, (list,)):
        collectionid = ",".join(collectionid)

    req = requests.get("https://nda.nih.gov/api/submission/",
                       params={'usersOwnSubmissions': users_own_submissions,
                               'collectionId': collectionid,
                               'status': status},
                       auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for collection %s" % (req.url, collectionid))

    if req.ok:
        return req.json()
    else:
        logger.debug(f"{req.status_code} - {req.url} - {req.text}")
        return None


def get_submission_files(auth, submissionid: int,
                         submission_file_status: str = "Complete",
                         retrieve_files_to_upload: bool = False) -> dict:
    """Use the NDA Submission API to get files for an NDA submission.
    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        submissionid: An NDA collection ID or a list of NDA collection IDs. If None, gets all submissions.
        submission_file_status: Status of submission files to retrieve, If None, gets all files.
        retrieve_files_to_upload: Flag indicating that only files that need to be uploaded be retrived.
    Returns:
        dict from JSON format.

    """

    req = requests.get(f"https://nda.nih.gov/api/submission/{submissionid}/files",  # pylint: disable=line-too-long
                       params={'submissionFileStatus': submission_file_status,
                               'retrieveFilesToUpload': retrieve_files_to_upload},
                       auth=auth, headers={'Accept': 'application/json'})

    logger.debug(f"Request {req.url} for submission {submissionid}")

    if req.ok:
        return req.json()
    else:
        logger.debug(f"{req.status_code} - {req.url} - {req.text}")
        return None


def get_experiment(auth, experimentid: int) -> dict:
    """Use the NDA Experiment API to get an experiment.
    Args:
        auth: a requests.auth.HTTPBasicAuth object to connect to NDA.
        experimentid: An NDA collection ID or a list of NDA collection IDs.
                      If None, gets all submissions.
    Returns:
        dict from JSON format.
    """

    req = requests.get(f"https://nda.nih.gov/api/experiment/{experimentid}",
                       auth=auth, headers={'Accept': 'application/json'})

    logger.debug(f"Request {req.url} for experiment {experimentid}")

    if req.ok:
        return req.json()
    else:
        logger.debug(f"{req.status_code} - {req.url} - {req.text}")
        return None

def process_submissions(submission_data):
    """Process NDA submissions from the NDA Submission API.

    The specific NDA API is the root submission endpoint that gets submissions
    from specific collections.

    Args:
        submission_data: Dictionary of data from NDA Submission API,
                         or from ndasynapse.nda.get_submissions
    Returns:
        Pandas data frame with submission information.
    """

    if submission_data is None:
        logger.debug("No submission data to process.")
        return pandas.DataFrame()

    if not isinstance(submission_data, (list,)):
        submission_data = [submission_data]
    
    submissions = [dict(collectionid=x['collection']['id'],
                        collectiontitle=x['collection']['title'],
                        submission_id=x['submission_id'],
                        submission_status=x['submission_status'],
                        dataset_title=x['dataset_title']) for x in submission_data]  # pylint: disable=line-too-long

    return pandas.DataFrame(submissions)


def split_bucket_and_key(s3_path):
    if not s3_path.startswith("s3://"):
        raise ValueError("Path does not start with s3://.")

    bucket, key = (s3_path
                   .split('//')[1]
                   .split('/', 1))
    return {'bucket': bucket, 'key': key}


NDA_STANDARD_DS_ENDPOINTS = ('gpop', 'NDAR_Central_1', 'NDAR_Central_2',
                             'NDAR_Central_3', 'NDAR_Central_4')


def nda_bsmn_location(remote_path, collection_id, submission_id):
    """Get the location of the duplicated data in the BSMN data enclave.

    This is only available if the data is in one of the NDA standard
    data submission endpoints, defined by the variable
    NDA_STANDARD_DS_ENDPOINTS.
    """

    if remote_path is None:
        return None

    bucket_and_key = split_bucket_and_key(remote_path)

    if bucket_and_key['bucket'] in NDA_STANDARD_DS_ENDPOINTS:

        original_key = bucket_and_key['key'].replace('ndar_data/DataSubmissions',  # pylint: disable=line-too-long
                                                     'submission_{}/ndar_data/DataSubmissions'.format(submission_id))  # pylint: disable=line-too-long
        nda_bsmn_key = 'collection_{}/{}'.format(collection_id, original_key)
        bucket_and_key = {'bucket': 'nda-bsmn', 'key': nda_bsmn_key}

    return f"s3://{bucket_and_key['bucket']}/{bucket_and_key['key']}"


def process_submission_files(submission_files):

    submission_files_processed = [dict(id=x['id'],
                                       file_type=x['file_type'],
                                       file_remote_path=x['file_remote_path'],
                                       status=x['status'],
                                       md5sum=x['md5sum'],
                                       size=x['size'],
                                       created_date=x['created_date'],
                                       modified_date=x['modified_date'])
                                  for x in submission_files]

    return pandas.DataFrame(submission_files_processed)


def get_submission_ids_from_links(data_structure_row: dict) -> set:
    """Get a set of submission IDs from a row from the NDA GUID API.

    This requires that the data was submitted to one of NDA's
    standard data submission AWS S3 endpoints. Bucket names are defined in
    NDA_STANDARD_DS_ENDPOINTS.

    Args:
        data_structure_row: a dictionary from the NDA GUID data API.
    Returns:
        a set of submission IDs as integers.

    """

    submission_ids = set()
    for link_row in data_structure_row["links"]["link"]:
        if link_row["rel"].lower() == "data_file":
            bucket_and_key = split_bucket_and_key(link_row["href"])
            if bucket_and_key['bucket'] not in NDA_STANDARD_DS_ENDPOINTS:
                logger.warn("Found a file not submitted to an NDA standard endpoint. Not adding a submission ID.")  # pylint: disable=line-too-long
            else:
                submission_string = bucket_and_key['key'].split("/", 1)[0]
                submission_id = submission_string.replace("submission_", "")
                submission_id = int(submission_id)
                submission_ids.add(submission_id)

    if len(submission_ids) > 1:
        logger.warn(f"Found different submission ids: {submission_ids}")

    return submission_ids


def get_collection_ids_from_links(data_structure_row: dict) -> set:
    """Get a set of collection IDs from the NDA GUID API.

    Args:
        data_structure_row: a dictionary returned by the NDA GUID data API.
    Returns:
        a set of collection IDs as integers.

    """

    collection_ids = set()
    for link_row in data_structure_row["links"]["link"]:
        if link_row["rel"].lower() == "collection":
            collection_ids.add(int(link_row["href"].split("=")[1]))

    if len(collection_ids) > 1:
        logger.warn(f"Found different collection ids: {collection_ids}")

    return collection_ids

def get_experiment_ids_from_links(data_structure_row: dict) -> set:
    """Get a set of experiment IDs from the NDA GUID API.

    Args:
        data_structure_row: a dictionary returned by the NDA GUID data API.
    Returns:
        a set of experiment IDs as integers.

    """

    experiment_ids = set()
    for link_row in data_structure_row["links"]["link"]:
        if link_row["rel"].lower() == "experiment_id":
            experiment_ids.add(int(link_row["href"].split("=")[1]))

    if len(experiment_ids) > 1:
        logger.warn(f"Found different collection ids: {experiment_ids}")

    return experiment_ids


@deprecated(reason="This function is deprecated, use the function process_guid_data.")
def sample_data_files_to_df(guid_data):
    # Get data files from samples.
    tmp = []

    for row in guid_data['age'][0]['dataStructureRow']:

        collection_id = get_collection_ids_from_links(row).pop()
        dataset_id = row['datasetId']
        tmp_row_dict = {'collection_id': collection_id,
                        'datasetId': dataset_id}

        for col in row['dataElement']:
            tmp_row_dict[col['name']] = col['value']
            if col.get('md5sum') and col.get('size') and \
                col['name'].startswith('DATA_FILE'):
                tmp_row_dict["%s_md5sum" % (col['name'], )] = col['md5sum']
                tmp_row_dict["%s_size" % (col['name'], )] = col['size']
        tmp.append(tmp_row_dict)

    samples = pandas.io.json.json_normalize(tmp)

    return samples

def extract_from_cdata(string):
    """Extract the value out of an XML CDATA section.

    See https://en.wikipedia.org/wiki/CDATA#CDATA_sections_in_XML.

    """
    tmp = string.lstrip("<![CDATA[")
    tmp = tmp.rstrip("]]>")
    return tmp

SHORT_NAMES = ("genomics_subject02", "nichd_btb02", "genomics_sample03")
SHORT_NAME_ID_COLS = [f"{short_name}_id".upper() for short_name in SHORT_NAMES]

def process_guid_data(guid_data, collection_ids=None, drop_duplicates=False):
    """Process the GUID data into a data frame.

    This takes all values from the 'dataElement' records and adds them
    as columns in a data frame. If the element is a data file, it extracts
    the URL out of a CDATA XML section.

    The NDA collection is added to each row in the data frame as well.

    The documentation for the data structure is here:
    https://nda.nih.gov/api/guid/docs/swagger-ui.html#!/guid/guidXMLTableUsingGET

    Args:
        guid_data: A dictionary from the output of the NDA GUID service.
        collection_ids: a list of collection IDs to filter records on.
                        If None, no filtering.
        drop_duplicates: Return unique rows after removing the primary key
                         from the data. The primary key of each is determined by
                         it's manifest short name plus the string "ID"
                         (for example, "GENOMICS_SUBJECT02_ID").
    Returns:
        A data frame with processed values from the dataElement records
        plus other metadata about it's source from the guid data record.

    """

    data = []

    for ds_row in guid_data["age"][0]["dataStructureRow"]:

        dataset_id = str(ds_row['datasetId'])

        found_collection_ids = get_collection_ids_from_links(
            data_structure_row=ds_row)

        # Check to see if this data comes from the provided collections
        if collection_ids and not found_collection_ids.intersection(collection_ids):
            continue
        else:
            found_collection_ids = ",".join(
                [str(x) for x in found_collection_ids])

        submission_ids = get_submission_ids_from_links(
            data_structure_row=ds_row)
        submission_ids = ",".join([str(x) for x in submission_ids])
        logger.debug(f"Submission IDs: {submission_ids}")

        manifest_data = dict(collection_id=found_collection_ids,
                             submission_id=submission_ids,
                             datasetid=dataset_id)

        # Get all of the metadata
        for de_row in ds_row["dataElement"]:

            manifest_data[de_row['name']] = de_row['value']

            if de_row.get('md5sum') and de_row.get('size') and \
                de_row['name'].startswith('DATA_FILE'):
                manifest_data[de_row["name"]] = extract_from_cdata(de_row['value'])
                logger.debug(manifest_data)
                manifest_data["%s_bsmn_location" % (de_row['name'], )] = \
                    nda_bsmn_location(remote_path=manifest_data[de_row["name"]],
                                      collection_id=manifest_data['collection_id'],
                                      submission_id=manifest_data['submission_id'])
                manifest_data["%s_md5sum" % (de_row['name'], )] = de_row['md5sum']
                manifest_data["%s_size" % (de_row['name'], )] = de_row['size']

        manifest_flat_df = pandas.io.json.json_normalize(manifest_data)
        data.append(manifest_flat_df)

    # Get the manifest data dictionary into a dataframe and
    # flatten it out if necessary.
    try:
        all_guids_df = pandas.concat(data, axis=0, ignore_index=True,
                                     sort=False)
    except ValueError:
        logger.warning("No records found.")
        return pandas.DataFrame()

    if drop_duplicates:
        # Get rid of any rows that are exact duplicates except for
        # the manifest ID column
        drop_cols = [col for col in all_guids_df.columns if col in SHORT_NAME_ID_COLS]  # pylint: disable=line-too-long
        all_guids_df.drop(drop_cols, axis=1, inplace=True)
        column_list = (all_guids_df.columns).tolist()
        all_guids_df = all_guids_df.drop_duplicates(subset=column_list,
                                                    keep="first")

    return all_guids_df


def process_samples(samples):

    colnames_lower = [x.lower() for x in samples.columns.tolist()]
    samples.columns = colnames_lower

    logger.debug(f"All column names: {colnames_lower}")

    datafile_column_names = samples.filter(regex=r"data_file\d+$").columns.tolist()  # pylint: disable=line-too-long

    samples_final = pandas.DataFrame()
    sample_columns = [col for col in samples.columns.tolist() if not col.startswith("data_file")]  # pylint: disable=line-too-long

    for col in datafile_column_names:
        keep_cols = sample_columns + \
            [col, f'{col}_type', f'{col}_md5sum', f'{col}_size']
        samples_tmp = samples[keep_cols]

        samples_tmp.rename(columns={col: 'data_file',
                                    f'{col}_type': 'fileFormat',
                                    f'{col}_md5sum': 'md5',
                                    f'{col}_size': 'size'},
                           inplace=True)

        samples_final = pandas.concat([samples_final, samples_tmp],
                                      ignore_index=True)

    missing_data_file = samples_final.data_file.isnull()

    missing_files = samples_final.datasetid[missing_data_file].drop_duplicates().tolist()  # pylint: disable=line-too-long

    if missing_files:
        logger.info("These datasets are missing a data file and will be dropped: %s" % (missing_files,))  # pylint: disable=line-too-long
        samples_final = samples_final[~missing_data_file]

    samples_final['fileFormat'].replace(['BAM', 'FASTQ', 'bam_index'],
                                        ['bam', 'fastq', 'bai'],
                                        inplace=True)

    # # Remove initial slash to match what is in manifest file
    # samples_final.data_file = samples_final['data_file'].apply(lambda value: value[1:] if not pandas.isnull(value) else value)  # pylint: disable=line-too-long

    # # Remove stuff that isn't part of s3 path
    # samples_final.data_file = [str(x).replace("![CDATA[", "").replace("]]>", "")  # pylint: disable=line-too-long
    #                            for x in samples_final.data_file.tolist()]

    samples_final = samples_final[samples_final.data_file != 'nan']

    samples_final['species'] = samples_final.organism.replace(['Homo Sapiens'],
                                                              ['Human'])

    # df.drop(["organism"], axis=1, inplace=True)

    # df = df[SAMPLE_COLUMNS]

    return samples_final


def subjects_to_df(json_data):

    tmp = []

    for row in json_data['age'][0]['dataStructureRow']:
        collection_id = get_collection_ids_from_links(row).pop()

        new_row = {col['name']: col['value'] for col in row['dataElement']}
        new_row['collection_id'] = collection_id
        tmp.append(new_row)

    df = pandas.io.json.json_normalize(tmp)

    colnames_lower = map(lambda x: x.lower(), df.columns.tolist())
    df.columns = colnames_lower

    return df


def process_subjects(df, exclude_genomics_subjects=[]):
    # For some reason there are different ids for this that aren't usable
    # anywhere, so dropping them for now
    # Exclude some subjects
    df = df[~df.genomics_subject02_id.isin(exclude_genomics_subjects)]
    # df.drop(["genomics_subject02_id"], axis=1, inplace=True)

    try:
        df['sex'] = df['sex'].replace(['M', 'F'], ['male', 'female'])
    except KeyError:
        logger.error(f"Key 'sex' not found in data frame. Available columns: {df.columns}")  # pylint: disable=line-too-long
        logger.error(f"Trying to use 'gender' and add new 'sex' column.")
        df['sex'] = df['gender'].replace(['M', 'F'], ['male', 'female'])
        # df = df.drop(labels='gender', axis=1, inplace=True)

    df = df.assign(subject_sample_id_original=df.sample_id_original,
                   subject_biorepository=df.biorepository)

    df.drop(["sample_id_original", "biorepository"], axis=1, inplace=True)

    df = df.drop_duplicates()

    # df = df[SUBJECT_COLUMNS]

    return df


def tissues_to_df(json_data):
    tmp = []

    for row in json_data['age'][0]['dataStructureRow']:
        collection_id = get_collection_ids_from_links(row).pop()

        new_row = {col['name']: col['value'] for col in row['dataElement']}
        new_row['collection_id'] = collection_id
        tmp.append(new_row)

    df = pandas.io.json.json_normalize(tmp)

    return df


def process_tissues(df):
    colnames_lower = map(lambda x: x.lower(), df.columns.tolist())
    df.columns = colnames_lower

    df['sex'] = df['sex'].replace(['M', 'F'], ['male', 'female'])

    # This makes them non-unique, so drop them
    # df.drop('nichd_btb02_id', axis=1, inplace=True)

    df = df.drop_duplicates()

    return df


def flattenjson(b, delim):
    val = {}
    for i in b.keys():
        if isinstance(b[i], dict):
            get = flattenjson(b[i], delim)
            for j in get.keys():
                val[i + delim + j] = get[j]
        else:
            val[i] = b[i]

    return val



def get_experiments(auth, experiment_ids):
    df = []

    logger.info("Getting experiments.")

    for experiment_id in experiment_ids:

        data = get_experiment(auth, experiment_id)
        data_flat = flattenjson(data[u'omicsOrFMRIOrEEG']['sections'], '.')
        data_flat['experiment_id'] = experiment_id

        df.append(data_flat)


    return df


def process_experiments(d):

    fix_keys = ['processing.processingKits.processingKit',
                'additionalinformation.equipment.equipmentName',
                'extraction.extractionKits.extractionKit',
                'additionalinformation.analysisSoftware.software']

    df = pandas.DataFrame()

    logger.info("Processing experiments.")

    for experiment in d:

        for key in fix_keys:
            foo = experiment[key]
            tmp = ",".join(map(lambda x: "%s %s" % (x['vendorName'], x['value']), foo))
            experiment[key] = tmp

        foo = experiment['processing.processingProtocols.processingProtocol']
        tmp = ",".join(map(lambda x: "%s: %s" % (x['technologyName'], x['value']), foo))
        experiment['processing.processingProtocols.processingProtocol'] = tmp

        experiment['extraction.extractionProtocols.protocolName'] = ",".join(
            experiment['extraction.extractionProtocols.protocolName'])

        logger.debug("Processed experiment %s\n" % (experiment, ))

        expt_df = pandas.DataFrame(experiment, index=experiment.keys())

        df = df.append(expt_df, ignore_index=True)

    df_change = df[EXPERIMENT_COLUMNS_CHANGE.keys()]
    df_change = df_change.rename(columns=EXPERIMENT_COLUMNS_CHANGE, inplace=False)
    df2 = pandas.concat([df, df_change], axis=1)
    df2 = df2.rename(columns=lambda x: x.replace(".", "_"))
    df2['platform'] = df2['equipmentName'].replace(EQUIPMENT_NAME_REPLACEMENTS,
                                                   inplace=False)

    df2['assay'] = df2['applicationSubType'].replace(APPLICATION_SUBTYPE_REPLACEMENTS,
                                                     inplace=False)

    # Should be fixed at NDA
    df2['assay'][df2['experiment_id'].isin(['675', '777', '778'])] = "targetedSequencing"

    return df2


def merge_tissues_subjects(tissues, subjects):
    """Merge together the tissue file and the subjects file.

    We instituted a standard to use `sample_id_biorepository` in the
    `genomics_sample03` file to map to `sample_id_original` in the
    `nichd_btb02` file.

    """

    btb_subjects = tissues.merge(subjects, how="left",
                                 left_on=["src_subject_id", "subjectkey",
                                          "race", "sex"],
                                 right_on=["src_subject_id", "subjectkey",
                                           "race", "sex"])

    # Rename this column to simplify merging with the sample table
    btb_subjects = btb_subjects.assign(
        sample_id_biorepository=btb_subjects.sample_id_original)

    # Drop this as it will come back from the samples
    btb_subjects.drop('sample_id_original', axis=1, inplace=True)

    return btb_subjects


def merge_tissues_samples(btb_subjects, samples):
    """Merge the tissue/subject with the samples to make a complete metadata table."""

    metadata = samples.merge(btb_subjects, how="left",
                             left_on=["src_subject_id", "subjectkey",
                                      "sample_id_biorepository"],
                             right_on=["src_subject_id", "subjectkey",
                                       "sample_id_biorepository"])

    metadata = metadata.drop_duplicates()

    return metadata


@deprecated(reason="Should not depend on bucket location to get manifests. Use NDASubmissionFiles class.")
def get_manifests(bucket):
    """Get list of `.manifest` files from the NDA-BSMN bucket.

    Read them in and concatenate them, under the assumption that the files
    listed in the manifest are in the same directory as the manifest file
    itself.

    """

    objects = bucket.objects.all()
    manifests = [x for x in objects if x.key.find('.manifest') >= 0]

    manifest = pandas.DataFrame()

    for m in manifests:
        manifest_body = io.BytesIO(m.get()['Body'].read())
        folder = os.path.split(m.key)[0]

        try:
            tmp = pandas.read_csv(manifest_body, delimiter="\t", header=None)
        except pandas.errors.EmptyDataError:
            logger.info("No data in the manifest for %s" % (m,))
            continue

        tmp.columns = MANIFEST_COLUMNS
        tmp.filename = "s3://%s/%s/" % (bucket.name, folder,) + tmp.filename.map(str)
        manifest = pandas.concat([manifest, tmp])

    manifest.reset_index(drop=True, inplace=True)

    return manifest


def merge_metadata_manifest(metadata, manifest):
    metadata_manifest = manifest.merge(metadata, how="left",
                                       left_on="filename",
                                       right_on="data_file")

    metadata_manifest = metadata_manifest.drop_duplicates()

    return metadata_manifest


def find_duplicate_filenames(metadata):
    """Find duplicates based on the basename of the data_file column.

    """
    basenames = metadata.data_file.apply(lambda x: os.path.basename(x))
    counts = basenames.value_counts()

    duplicates = counts[counts > 1].index

    return (metadata[~basenames.isin(duplicates)],
            metadata[basenames.isin(duplicates)])

def get_manifest_file_data(data_files, manifest_type):
    for data_file in data_files:

        data_file_as_string = data_file["content"].decode("utf-8")

        if manifest_type in data_file_as_string:
            manifest_df = pandas.read_csv(io.StringIO(data_file_as_string),
                                          skiprows=1)
            return manifest_df

    return None

class NDASubmissionFiles:

    ASSOCIATED_FILE = 'Submission Associated File'
    DATA_FILE = 'Submission Data File'
    MANIFEST_FILE = 'Submission Manifest File'
    SUBMISSION_PACKAGE = 'Submission Data Package'
    SUBMISSION_TICKET = 'Submission Ticket'
    SUBMISSION_MEMENTO = 'Submission Memento'

    logger = logging.getLogger('NDASubmissionFiles')
    logger.setLevel(logging.INFO)

    def __init__(self, auth, files, collection_id, submission_id):
        self.auth = auth
        self.headers = {'Accept': 'application/json'}
        self.collection_id = str(collection_id)
        self.submission_id = str(submission_id)

        (self.associated_files,
         self.data_files,
         self.manifest_file,
         self.submission_package,
         self.submission_ticket,
         self.submission_memento) = self.get_nda_submission_file_types(files)

        self.bsmn_locations = [nda_bsmn_location(x.get('remote_path', None),
                                                 self.collection_id,
                                                 self.submission_id)
                               for x in files]

        self.debug = True

    def get_nda_submission_file_types(self, files):
        associated_files = []
        data_files = []
        manifest_file = []
        submission_package = []
        submission_ticket = []
        submission_memento = []

        for file in files:
            if file['file_type'] == self.ASSOCIATED_FILE:
                associated_files.append({'name': file})
            elif file['file_type'] == self.DATA_FILE:
                data_files.append({'name': file,
                                   'content': self.read_file(file)})
            elif file['file_type'] == self.MANIFEST_FILE:
                manifest_file.append({'name': file,
                                      'content': self.read_file(file)})
            elif file['file_type'] == self.SUBMISSION_PACKAGE:
                submission_package.append(file)
            elif file['file_type'] == self.SUBMISSION_TICKET:
                submission_ticket.append({'name': file,
                                          'content': self.read_file(file)})
            elif file['file_type'] == self.SUBMISSION_MEMENTO:
                submission_memento.append({'name': file,
                                           'content': self.read_file(file)})

        return (associated_files,
                data_files,
                manifest_file,
                submission_package,
                submission_ticket,
                submission_memento)

    def read_file(self, submission_file):
        download_url = submission_file['_links']['download']['href']
        request = requests.get(download_url, auth=self.auth)

        return request.content

    def manifest_to_df(self, short_name):
        """Read the contents of a data file given by the short name.

        Args:
            short_name: An NDA short name for a manifest type (like 'genomics_sample03').
        Returns:
            Pandas data frame, or None if no data file found.
        """
        logger.warning("Information in the submission manifests may be out of date with respect to the NDA database.")

        for data_file in self.data_files:
            data_file_as_string = data_file['content'].decode('utf-8')
            if short_name in data_file_as_string:
                data = pandas.read_csv(io.StringIO(data_file_as_string),
                                       skiprows=1)
                return data

        return None


class NDASubmission:

    _subject_manifest = "genomics_subject"
    _sample_manifest = "genomics_sample"

    logger = logging.getLogger('NDASubmission')
    logger.setLevel(logging.INFO)

    def __init__(self, auth, submission_id):

        self.auth = auth
        self.submission_id = str(submission_id)
        self.submission = get_submission(auth=self.auth,
                                         submissionid=submission_id)

        if self.submission is None:
            self.logger.error(f"Could not retrieve submission {self.submission_id}.")
            self.processed_submissions = None
            self.submission_files = None
            self.guids = set()
        else:
            self.processed_submission = process_submissions(
                submission_data=self.submission)
            self.submission_files = self.get_submission_files()
            self.guids = self.get_guids()
            self.logger.info(f"Got submission {self.submission_id}.")

    def get_submission_files(self):
        submission_id = str(self.submission['submission_id'])
        collection_id = str(self.submission['collection']['id'])

        files = get_submission_files(auth=self.auth,
                                     submissionid=submission_id)
        processed_files = process_submission_files(submission_files=files)
        processed_files['submission_id'] = submission_id
        processed_files['collection_id'] = collection_id

        sub_files = {'files': NDASubmissionFiles(auth=self.auth,
                                                 files=files,
                                                 collection_id=collection_id,
                                                 submission_id=submission_id),
                     'processed_files': processed_files,
                     'collection_id': collection_id,
                     'submission_id': submission_id}

        return sub_files


    def get_guids(self):
        """Get a list of GUIDs for each submission.

        Uses the genomics subject manifest data file.

        This requires looking inside the submission-associated data file
        to find the GUIDs. It is prone to issues of being outdated due to
        submission edits.

        """
        logger.warning("GUID information comes from the submission manifests may be out of date with respect to the NDA database.")

        guids = set()

        submission_data_files = self.submission_files["files"].data_files
        manifest_df = get_manifest_file_data(submission_data_files,
                                             self._sample_manifest)

        if manifest_df is None:
            self.logger.debug(f"No {self._sample_manifest} manifest for submission {self.submission_id}. Looking for the {self._subject_manifest} manifest.")
            manifest_df = get_manifest_file_data(submission_data_files,
                                                 self._subject_manifest)

        if manifest_df is not None:
            try:
                guids_found = manifest_df["subjectkey"].tolist()
                self.logger.debug(f"Adding {len(guids_found)} GUIDS for submission {self.submission_id}.")
                guids.update(guids_found)
            except KeyError:
                self.logger.error(f"Manifest for submission {self.submission_id} had no guid (subjectkey) column.")
        else:
            self.logger.info(f"No manifest with GUIDs found for submission {self.submission_id}")

        return guids


class NDACollection(object):

    _subject_manifest = "genomics_subject"
    _sample_manifest = "genomics_sample"

    logger = logging.getLogger('NDACollection')
    logger.setLevel(logging.INFO)

    def __init__(self, auth, collection_id=None):

        self.auth = auth
        self.collection_id = str(collection_id)

        self._collection_submissions = get_submissions(auth=self.auth,
                                                       collectionid=self.collection_id)

        self.logger.info(f"Getting {len(self._collection_submissions)} submissions for collection {self.collection_id}.")

        self.submissions = []

        for coll_sub in self._collection_submissions:
            if coll_sub is not None:
                sub = NDASubmission(auth=self.auth,
                                    submission_id=coll_sub['submission_id'])
                if sub.submission is not None:
                    self.submissions.append(sub)

        self.submission_files = self.get_submission_files()
        self.guids = self.get_guids()
        self.logger.info(f"Got collection {self.collection_id}.")

    def get_submission_files(self):
        submission_files = []
        for submission in self.submissions:
            if submission.submission_files is not None:
                submission_files.extend(submission.submission_files)
        return submission_files


    def get_guids(self):
        """Get a list of GUIDs for each submission.

        Uses the genomics subject manifest data file.

        This requires looking inside the submission-associated data file to find the GUIDs.
        It is prone to issues of being outdated due to submission edits.

        """
        logger.warning("GUID information comes from the submission manifests may be out of date with respect to the NDA database.")

        guids = set()
        for submission in self.submissions:
            guids.update(submission.guids)

        return guids

    def get_collection_manifests(self, manifest_type):
        """Get all original manifests submitted with each submission in a collection.

        NDA does not update these files if metadata change requests are made.
        They only update metadata in their database, accessible through the GUID API.
        Records obtained here should be used for historical purposes only.

        Args:
            manifest_type: An NDA manifest type, like 'genomics_sample'.
        Returns:
            A pandas data frame of all submission manifests concatenated together.
        """

        logger.warning("Information in the collection manifests may be out of date.")

        all_data = []

        for submission in self.submissions:
            logger.debug(f"Getting manifests for submission {submission.submission_id}")
            try:
                ndafiles = submission.submission_files['files']
            except (IndexError, TypeError):
                logger.info(f"No submission files for collection {coll_id}.")
                continue

            manifest_data = ndafiles.manifest_to_df(manifest_type)

            if manifest_data is not None and manifest_data.shape[0] > 0:
                manifest_data['collection_id'] = str(self.collection_id)
                manifest_data['submission_id'] = str(submission.submission_id)
                all_data.append(manifest_data)
            else:
                logger.info(f"No {manifest_type} data found for submission {submission.submission_id}.")

        if all_data:
            all_data_df = pandas.concat(all_data, axis=0, ignore_index=True, sort=False)
            return all_data_df

        return pandas.DataFrame()
