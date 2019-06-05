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

# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# logger.addHandler(ch)

METADATA_COLUMNS = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original',
                    'sample_id_biorepository', 'subject_sample_id_original', 'biorepository',
                    'subject_biorepository', 'sample_description', 'species', 'site', 'sex',
                    'sample_amount', 'phenotype',
                    'comments_misc', 'sample_unit', 'fileFormat']

SAMPLE_COLUMNS = ['datasetid', 'experiment_id', 'sample_id_original', 'storage_protocol',
                  'sample_id_biorepository', 'organism', 'sample_amount', 'sample_unit',
                  'biorepository', 'comments_misc', 'site', 'genomics_sample03_id',
                  'src_subject_id', 'subjectkey']

SUBJECT_COLUMNS = ['src_subject_id', 'subjectkey', 'sex', 'race', 'phenotype',
                   'subject_sample_id_original', 'sample_description', 'subject_biorepository',
                   'sex']

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
    # # Credential configuration for NDA
    
    ndaconfig = config['nda']
    
    auth = requests.auth.HTTPBasicAuth(ndaconfig['username'], ndaconfig['password'])
    
    return auth


def get_samples(auth, guid):
    """Use the NDA api to get the `genomics_sample03` records for a GUID."""

    r = requests.get("https://nda.nih.gov/api/guid/{}/data?short_name=genomics_sample03".format(guid),
                     auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for GUID %s" % (r, guid))

    if r.status_code != 200:
        raise requests.HTTPError(r.json())

    return r.json()

def get_submissions(auth, collectionid, users_own_submissions=False):
    """Use the NDA api to get the `genomics_sample03` records for a GUID."""

    r = requests.get("https://nda.nih.gov/api/submission/",
                     params={'usersOwnSubmissions': users_own_submissions,
                             'collectionId': collectionid},
                     auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for collection %s" % (r.url, collectionid))

    if r.status_code != 200:
        logger.debug(r.status_code)
        raise requests.HTTPError(r.json())

    return r.json()

def process_submissions(submission_data):
    """Process submissions from nested JSON to a data frame.
    """

    if not isinstance(submission_data, (list,)):
        submission_data = [submission_data]
    
    submissions =  [dict(collectionid=x['collection']['id'], collectiontitle=x['collection']['title'],
                         submission_id=x['submission_id'], submission_status=x['submission_status'],
                         dataset_title=x['dataset_title']) for x in submission_data]

    return pandas.DataFrame(submissions)

def get_submission(auth, submissionid):
    """Use the NDA api to get the `genomics_sample03` records for a GUID."""

    r = requests.get("https://nda.nih.gov/api/submission/{}".format(submissionid),
                     auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for submission %s" % (r, submissionid))

    if r.status_code != 200:
        raise requests.HTTPError("{} - {} - {}".format(r.status_code, r.url, r.body))

    return r.json()

def get_submission_files(auth, submissionid, submission_file_status="Complete", retrieve_files_to_upload=False):
    """Use the NDA api to get the `genomics_sample03` records for a GUID."""

    r = requests.get("https://nda.nih.gov/api/submission/{}/files".format(submissionid),
                     params={'submissionFileStatus': submission_file_status,
                             'retrieveFilesToUpload': retrieve_files_to_upload},
                     auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for submission %s" % (r, submissionid))

    if r.status_code != 200:
        raise requests.HTTPError("{} - {} - {}".format(r.status_code, r.url, r.body))

    return r.json()

def process_submission_files(submission_files):

    submission_files_processed = [dict(id=x['id'], file_type=x['file_type'], file_remote_path=x['file_remote_path'],
                                       status=x['status'], md5sum=x['md5sum'], size=x['size'],
                                       created_date=x['created_date'], modified_date=x['modified_date']) for x in submission_files]

    return pandas.DataFrame(submission_files_processed)

def get_sample_data_files(guid_data):
    # Get data files from samples.
    tmp = []

    for row in guid_data['age'][0]['dataStructureRow']:
        tmp_row_dict = {}
        for col in row['dataElement']:
            tmp_row_dict[col['name']] = col['value']
            if col.get('md5sum') and col.get('size') and col['name'].startswith('DATA_FILE'):
                tmp_row_dict["%s_md5sum" % (col['name'], )] = col['md5sum']
                tmp_row_dict["%s_size" % (col['name'], )] = col['size']
        tmp.append(tmp_row_dict)

    samples = pandas.io.json.json_normalize(tmp)
    samples['datasetId'] = [x['datasetId'] for x in guid_data['age'][0]['dataStructureRow']]

    return samples

def process_samples(samples):

    colnames_lower = [x.lower() for x in samples.columns.tolist()]
    samples.columns = colnames_lower

    datafile_column_names = samples.filter(regex="data_file\d+$").columns.tolist()

    samples_final = pandas.DataFrame()

    for col in datafile_column_names:
        sample_columns = [x for x in SAMPLE_COLUMNS if x in samples.columns]
        samples_tmp = samples[sample_columns + [col, '%s_type' % col, '%s_md5sum' % col, '%s_size' % col]]

        samples_tmp.rename(columns={col: 'data_file',
                                    '%s_type' % col: 'fileFormat',
                                    '%s_md5sum' % col: 'md5',
                                    '%s_size' % col: 'size'},
                           inplace=True)

        samples_final = pandas.concat([samples_final, samples_tmp], ignore_index=True)

    missing_data_file = samples_final.data_file.isnull()

    missing_files = samples_final.datasetid[missing_data_file].drop_duplicates().tolist()

    if missing_files:
        logger.info("These datasets are missing a data file and will be dropped: %s" % (missing_files,))
        samples_final = samples_final[~missing_data_file]
    
    samples_final['fileFormat'].replace(['BAM', 'FASTQ', 'bam_index'],
                                        ['bam', 'fastq', 'bai'],
                                        inplace=True)

    # Remove initial slash to match what is in manifest file
    samples_final.data_file = samples_final['data_file'].apply(lambda value: value[1:] if not pandas.isnull(value) else value)

    # Remove stuff that isn't part of s3 path
    samples_final.data_file = [str(x).replace("![CDATA[", "").replace("]]>", "") for x in samples_final.data_file.tolist()]

    samples_final = samples_final[samples_final.data_file != 'nan']

    samples_final['species'] = samples_final.organism.replace(['Homo Sapiens'], ['Human'])

    # df.drop(["organism"], axis=1, inplace=True)

    # df = df[SAMPLE_COLUMNS]

    return samples_final


def get_subjects(auth, guid):
    """Use the NDA API to get the `genomics_subject02` records for this GUID."""

    r = requests.get("https://nda.nih.gov/api/guid/{}/data?short_name=genomics_subject02".format(guid),
                     auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for GUID %s" % (r, guid))
    
    if r.status_code != 200:
        raise requests.HTTPError("{} - {} - {}".format(r.status_code, r.url, r.body))
    
    return r.json()

def subjects_to_df(json_data):

    tmp = []

    for row in json_data['age'][0]['dataStructureRow']:
        foo = {col['name']: col['value'] for col in row['dataElement']}
        tmp.append(foo)

    df = pandas.io.json.json_normalize(tmp)

    colnames_lower = map(lambda x: x.lower(), df.columns.tolist())
    df.columns = colnames_lower

    return df


def process_subjects(df, exclude_genomics_subjects=[]):
    # For some reason there are different ids for this that aren't usable
    # anywhere, so dropping them for now
    # Exclude some subjects
    df = df[~df.genomics_subject02_id.isin(exclude_genomics_subjects)]
    df.drop(["genomics_subject02_id"], axis=1, inplace=True)

    df['sex'] = df['sex'].replace(['M', 'F'], ['male', 'female'])

    df = df.assign(subject_sample_id_original=df.sample_id_original,
                   subject_biorepository=df.biorepository)

    df.drop(["sample_id_original", "biorepository"], axis=1, inplace=True)

    df = df.drop_duplicates()

    # df = df[SUBJECT_COLUMNS]

    return df


def get_tissues(auth, guid):
    """Use the NDA api to get the `ncihd_btb02` records for this GUID."""

    r = requests.get("https://nda.nih.gov/api/guid/{}/data".format(guid),
                     params={"short_name": "nichd_btb02"},
                     auth=auth, headers={'Accept': 'application/json'})

    logger.debug("Request %s for GUID %s" % (r, guid))

    if r.status_code != 200:
        raise requests.HTTPError("{} - {} - {}".format(r.status_code, r.url, r.body))
    
    return r.json()

def tissues_to_df(json_data):
    tmp = []
    
    for row in json_data['age'][0]['dataStructureRow']:
        foo = {col['name']: col['value'] for col in row['dataElement']}
        tmp.append(foo)

    df = pandas.io.json.json_normalize(tmp)

    return df


def process_tissues(df):
    colnames_lower = map(lambda x: x.lower(), df.columns.tolist())
    df.columns = colnames_lower

    df['sex'] = df['sex'].replace(['M', 'F'], ['male', 'female'])

    # This makes them non-unique, so drop them
    df.drop('nichd_btb02_id', axis=1, inplace=True)

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

def get_experiment(auth, experiment_id, verbose=False):

    url = "https://nda.nih.gov/api/experiment/{}".format(experiment_id)
    r = requests.get(url, auth=auth, headers={'Accept': 'application/json'})

    if r.status_code != 200:
        raise requests.HTTPError("{} - {} - {}".format(r.status_code, r.url, r.body))
    
    return r.json()


def get_experiments(auth, experiment_ids, verbose=False):
    df = []

    logger.info("Getting experiments.")

    for experiment_id in experiment_ids:

        data = get_experiment(auth, experiment_id, verbose=verbose)
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

    We instituted a standard to use `sample_id_biorepository` in the `genomics_sample03`
    file to map to `sample_id_original` in the `nichd_btb02` file.

    """

    btb_subjects = tissues.merge(subjects, how="left",
                                 left_on=["src_subject_id", "subjectkey", "race", "sex"],
                                 right_on=["src_subject_id", "subjectkey", "race", "sex"])

    # Rename this column to simplify merging with the sample table
    btb_subjects = btb_subjects.assign(sample_id_biorepository=btb_subjects.sample_id_original)

    # Drop this as it will come back from the samples
    btb_subjects.drop('sample_id_original', axis=1, inplace=True)

    return btb_subjects


def merge_tissues_samples(btb_subjects, samples):
    """Merge the tissue/subject with the samples to make a complete metadata table."""

    metadata = samples.merge(btb_subjects, how="left",
                             left_on=["src_subject_id", "subjectkey", "sample_id_biorepository"],
                             right_on=["src_subject_id", "subjectkey", "sample_id_biorepository"])

    metadata = metadata.drop_duplicates()

    return metadata


@deprecated(reason="Should not depend on bucket location to get manifests. Use NDASubmissionFiles class.")
def get_manifests(bucket):
    """Get list of `.manifest` files from the NDA-BSMN bucket.

    Read them in and concatenate them, under the assumption that the files listed
    in the manifest are in the same directory as the manifest file itself.

    """

    manifests = [x for x in bucket.objects.all() if x.key.find('.manifest') >=0]

    manifest = pandas.DataFrame()

    for m in manifests:
        manifest_body = io.BytesIO(m.get()['Body'].read())
        folder = os.path.split(m.key)[0]

        try:
            tmp = pandas.read_csv(manifest_body, delimiter="\t", header=None)
        except pandas.errors.EmptyDataError as e:
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

class NDASubmissionFiles:

    ASSOCIATED_FILE = 'Submission Associated File'
    DATA_FILE = 'Submission Data File'
    MANIFEST_FILE = 'Submission Manifest File'
    SUBMISSION_PACKAGE = 'Submission Data Package'
    SUBMISSION_TICKET = 'Submission Ticket'
    SUBMISSION_MEMENTO = 'Submission Memento'

    def __init__(self, config, files):
        self.config = config # ApplicationProperties().get_config
        self.submission_api = self.config.get('submission.service.url')
        self.auth = (self.config.get('username'),
                     self.config.get('password'))
        self.headers = {'Accept': 'application/json'}
        (self.associated_files,
         self.data_files,
         self.manifest_file,
         self.submission_package,
         self.submission_ticket,
         self.submission_memento) = self.get_nda_submission_file_types(files)
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
        request = requests.get(
            download_url,
            auth=self.auth
        )
        return request.content


class NDASubmission:

    def __init__(self, config, submission_id=None, collection_id=None):

        self.config = config # ApplicationProperties().get_config
        self.submission_api = self.config.get('submission.service.url')
        self.auth = (self.config.get('username'),
                     self.config.get('password'))
        self.headers = {'Accept': 'application/json'}
        self.collection_id = collection_id
        if collection_id:
            self.submissions = self.get_submissions_for_collection()
        else:
            self.submissions = [submission_id]

        self.submission_files = self.get_submission_files()

    def get_submissions_for_collection(self, status="Upload Completed"):

        request = requests.get(
            self.submission_api,
            params={'collectionId': self.collection_id,
                    'usersOwnSubmissions': False,
                    'status': status},
            headers=self.headers,
            auth=self.auth
        )
        try:
            submissions = json.loads(request.text)
            
        except json.decoder.JSONDecodeError:
            logger.error('Error occurred retrieving submissions from collection {}'.format(self.collection_id))
            logger.error('Request ({}) returned {}'.format(request.url, request.text))
        return [s['submission_id'] for s in submissions]

    def get_submission_files(self):
        submission_files = []
        for s in self.submissions:
            request = requests.get(
                self.submission_api + '/{}'.format(s),
                headers=self.headers,
                auth=self.auth
            )

            logger.debug(request.url)
            
            try:
                collection_id = json.loads(request.text)['collection']['id']
            except json.decoder.JSONDecodeError:
                logger.error('Error occurred retrieving submission {}'.format(s))
                logger.error('Request ({}) returned {}'.format(request.url, request.text))

            files = []
            request = requests.get(
                self.submission_api + '/{}/files'.format(s),
                headers=self.headers,
                auth=self.auth
            )

            logger.debug(request.url)

            try:
                files = json.loads(request.text)
            except json.decoder.JSONDecodeError:
                logger.error('Error occurred retrieving files from submission {}'.format(s))
                logger.error('Request ({}) returned {}'.format(request.url, request.text))
            submission_files.append({'files': NDASubmissionFiles(self.config, files),
                                     'collection_id': collection_id,
                                     'submission_id': s})
        return submission_files

