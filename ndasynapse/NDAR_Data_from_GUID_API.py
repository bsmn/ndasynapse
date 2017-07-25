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

# This is an old genomics subject
EXCLUDE_GENOMICS_SUBJECTS = ('92027', )
# EXCLUDE_EXPERIMENTS = ('534', '535')
EXCLUDE_EXPERIMENTS = ()

METADATA_COLUMNS = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original',
                    'sample_id_biorepository', 'subject_sample_id_original', 'biorepository',
                    'subject_biorepository', 'sample_description', 'species', 'site', 'sex',
                    'sample_amount', 'phenotype', 'comments_misc', 'sample_unit', 'fileFormat']

SAMPLE_COLUMNS = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original',
                  'sample_id_biorepository', 'organism', 'species', 'sample_amount', 'sample_unit',
                  'biorepository', 'comments_misc', 'site']

SUBJECT_COLUMNS = ['src_subject_id', 'subjectkey', 'gender', 'race', 'phenotype',
                   'subject_sample_id_original', 'sample_description', 'subject_biorepository',
                   'sex']

NDA_BUCKET_NAME = 'nda-bsmn'

# Synapse configuration
synapse_data_folder = 'syn7872188'
synapse_data_folder_id = int(synapse_data_folder.replace('syn', ''))
storage_location_id = '9209'

content_type_dict = {'.gz': 'application/x-gzip', '.bam': 'application/octet-stream',
                     '.zip': 'application/zip'}


# # Credential configuration for NDA

# In[2]:

s3 = boto3.resource("s3")
obj = s3.Object('kdaily-lambda-creds.sagebase.org', 'ndalogs_config.json')

config = json.loads(obj.get()['Body'].read())

ndaconfig = config['nda']

get_nda_s3_session(username, password):
    tokengenerator = nda_aws_token_generator.NDATokenGenerator()
    mytoken = tokengenerator.generate_token(username, password)

    session = boto3.Session(aws_access_key_id=mytoken.access_key,
                            aws_secret_access_key=mytoken.secret_key,
                            aws_session_token=mytoken.session)

    s3_nda = session.resource("s3")

s3_nda = get_nda_s3_session(ndaconfig['username'], ndaconfig['password'])

# # Get Samples
#
# Use the NDA api to get the `genomics_sample03` records for this GUID.
def get_samples(auth, guid):
    r = requests.get("https://ndar.nih.gov/api/guid/{}/data?short_name=genomics_sample03".format(guid),
                     auth=auth, headers={'Accept': 'application/json'})

    guid_data = json.loads(r.text)

    # Get data files from samples. There are currently up to two files per row.
    tmp = [{col['name']: col['value'] for col in row['dataElement']}
           for row in guid_data['age'][0]['dataStructureRow']]

    samples = pandas.io.json.json_normalize(tmp)

    colnames_lower = map(lambda x: x.lower(), samples.columns.tolist())
    samples.columns = colnames_lower

    # exclude some experiments
    samples = samples[~samples.experiment_id.isin(EXCLUDE_EXPERIMENTS)]

    samples1 = samples[SAMPLE_COLUMNS + ['data_file1', 'data_file1_type']]

    samples1.rename(columns={'data_file1': 'data_file', 'data_file1_type': 'fileFormat'},
                    inplace=True)

    samples2 = samples[SAMPLE_COLUMNS + ['data_file2', 'data_file2_type']]

    samples2.rename(columns={'data_file2': 'data_file', 'data_file2_type': 'fileFormat'},
                    inplace=True)

    samples3 = pandas.concat([samples1, samples2], ignore_index=True)
    samples3.filter(~samples3.data_file.isnull())

    return samples3

def process_samples(df):
    df['fileFormat'].replace(['BAM', 'FASTQ', 'bam_index'],
                             ['bam', 'fastq', 'bai'],
                             inplace=True)

    # Remove initial slash to match what is in manifest file
    df.data_file = df['data_file'].apply(lambda value: value[1:] if not pandas.isnull(value) else value)

    # Remove stuff that isn't part of s3 path
    df.data_file = map(lambda x: str(x).replace("![CDATA[", "").replace("]]>", ""),
                             df.data_file.tolist())

    df = df[df.data_file != 'nan']

    df['species'] = df.organism.replace(['Homo Sapiens'], ['Human'])

    return df

def get_subjects(auth, guid):
    """Use the NDA API to get the `genomics_subject02` records for this GUID."""

    r = requests.get("https://ndar.nih.gov/api/guid/{}/data?short_name=genomics_subject02".format(guid),
                     auth=auth, headers={'Accept': 'application/json'})

    subject_guid_data = json.loads(r.text)

    for row in subject_guid_data['age'][0]['dataStructureRow']:
        foo = lambda row: {col['name']: col['value'] for col in row['dataElement']}
        tmp.append(foo)

    df = pandas.io.json.json_normalize(tmp)

    colnames_lower = map(lambda x: x.lower(), df.columns.tolist())
    df.columns = colnames_lower

    return df

def process_subjects(df):
    df = df[~df.GENOMICS_SUBJECT02_ID.isin(EXCLUDE_GENOMICS_SUBJECTS)]

    df = df.assign(sex=df.gender.replace(['M', 'F'], ['male', 'female']),
                   subject_sample_id_original=df.sample_id_original,
                   subject_biorepository=df.biorepository)

    df = df[SUBJECT_COLUMNS]

    df = df.drop_duplicates()

    return df

def get_tissues(auth, guid):
    """Use the NDA api to get the `ncihd_btb02` records for this GUID."""

    r = requests.get("https://ndar.nih.gov/api/guid/{}/data?short_name=nichd_btb02".format(guid),
                     auth=auth, headers={'Accept': 'application/json'})

    btb_guid_data = json.loads(r.text)

    tmp = []
    for row in btb_guid_data['age'][0]['dataStructureRow']:
        foo = {col['name']: col['value'] for col in row['dataElement']}
        tmp.append(foo)

    df = pandas.io.json.json_normalize(tmp)

    return df

def process_tissues(df):
    colnames_lower = map(lambda x: x.lower(), df.columns.tolist())
    df.columns = colnames_lower

    # This makes them non-unique, so drop them
    df.drop('nichd_btb02_id', axis=1, inplace=True)

    df = df.drop_duplicates()

    return df

def merge_tissues_subjects(tissues, subjects):
    """Merge together the tissue file and the subjects file.

    We instituted a standard to use `sample_id_biorepository` in the `genomics_sample03`
    file to map to `sample_id_original` in the `nichd_btb02` file.

    """

    btb_subjects = tissues.merge(subjects, how="left",
                                 left_on=["src_subject_id", "subjectkey", "race", "gender"],
                                 right_on=["src_subject_id", "subjectkey", "race", "gender"])

    # Rename this column to simplify merging with the sample table
    btb_subjects = btb_subjects.assign(sample_id_biorepository=btb_subjects.sample_id_original)

    # Drop this as it will come back from the samples
    btb_subjects.drop('sample_id_original', axis=1, inplace=True)

def merge_tissues_samples(btb_subjects, samples):
    """Merge the tissue/subject with the samples to make a complete metadata table."""

    metadata = samples.merge(btb_subjects, how="left",
                             left_on=["src_subject_id", "subjectkey", "sample_id_biorepository"],
                             right_on=["src_subject_id", "subjectkey", "sample_id_biorepository"])

    metadata = metadata.drop_duplicates()

def get_manifests(bucket):
    """Get list of `.manifest` files from the NDA-BSMN bucket.

    Read them in and concatenate them, under the assumption that the files listed
    in the manifest are in the same directory as the manifest file itself.
    """

    manifests = [x for x in bucket.objects.all() if x.key.find('.manifest') >=0]

    manifest = pandas.DataFrame()

    for m in manifests:
        folder = os.path.split(m.key)[0]
        tmp = pandas.read_csv(m.get()['Body'], delimiter="\t", header=None)
        tmp.columns = ('filename', 'md5', 'size')
        tmp.filename = "s3://%s/%s/" % (NDA_BUCKET_NAME, folder,) + tmp.filename.map(str)
        manifest = pandas.concat([manifest, tmp])

    manifest.reset_index(drop=True, inplace=True)

    return manifest

def merge_metadata_manifest(metadata, manifest):
    metadata_manifest = manifest.merge(metadata, how="left",
                                       left_on="filename",
                                       right_on="data_file")

    metadata_manifest = metadata_manifest.drop_duplicates()

    return metadata_manifest


# # Synapse
#
# Using the concatenated manifests as the master list of files to store, create file handles and entities in Synapse.
#
# Use the metadata table to get the appropriate tissue/subject/sample annotations to set on each File entity.

# In[28]:

# In[ ]:
auth = requests.auth.HTTPBasicAuth(ndaconfig['username'], ndaconfig['password'])
bucket = s3_nda.Bucket(NDA_BUCKET_NAME)

samples = get_samples(auth, guid=REFERENCE_GUID)
samples = process_samples(samples)
samples.to_csv("./samples.csv")

subjects = get_subjects(auth, REFERENCE_GUID)
subjects = process_subjects(subjects)

subjects.to_csv("./subjects.csv")

btb = get_tissues(auth, REFERENCE_GUID)
btb = process_tissues(btb)
btb.to_csv('./btb.csv')

btb_subjects = merge_tissues_subjects(btb, subjects)
btb_subjects.to_csv('btb_subjects.csv')

manifest = get_manifests(bucket)
# Only keep the files that are in the metadata table
manifest = manifest[manifest.filename.isin(metadata.data_file)]
manifest.to_csv('./manifest.csv')

metadata_manifest = merge_metadata_manifest(metadata, manifest)
metadata_manifest.to_csv('./metadata_manifest.csv')

fh_list = create_synapse_filehandles(metadata_manifest, NDA_BUCKET_NAME)

fh_ids = map(lambda x: x['id'], fh_list)
synapse_manifest = metadata_manifest[METADATA_COLUMNS]
synapse_manifest.path = fh_ids
f_list = store(synapse_manifest)

# a = a.to_dict()
