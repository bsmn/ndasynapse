import io
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
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

METADATA_COLUMNS = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original',
                    'sample_id_biorepository', 'subject_sample_id_original', 'biorepository',
                    'subject_biorepository', 'sample_description', 'species', 'site', 'sex',
                    'sample_amount', 'phenotype', 'comments_misc', 'sample_unit', 'fileFormat']

SAMPLE_COLUMNS = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original',
                  'sample_id_biorepository', 'organism', 'sample_amount', 'sample_unit',
                  'biorepository', 'comments_misc', 'site', 'genomics_sample03_id']

SUBJECT_COLUMNS = ['src_subject_id', 'subjectkey', 'gender', 'race', 'phenotype',
                   'subject_sample_id_original', 'sample_description', 'subject_biorepository',
                   'sex']

def get_nda_s3_session(username, password):
    tokengenerator = nda_aws_token_generator.NDATokenGenerator()
    mytoken = tokengenerator.generate_token(username, password)

    session = boto3.Session(aws_access_key_id=mytoken.access_key,
                            aws_secret_access_key=mytoken.secret_key,
                            aws_session_token=mytoken.session)

    s3_nda = session.resource("s3")

    return s3_nda

def get_samples(auth, guid):
    """Use the NDA api to get the `genomics_sample03` records for a GUID."""

    r = requests.get("https://ndar.nih.gov/api/guid/{}/data?short_name=genomics_sample03".format(guid),
                     auth=auth, headers={'Accept': 'application/json'})

    guid_data = json.loads(r.text)

    # Get data files from samples. There are currently up to two files per row.
    tmp = [{col['name']: col['value'] for col in row['dataElement']}
           for row in guid_data['age'][0]['dataStructureRow']]

    samples = pandas.io.json.json_normalize(tmp)

    colnames_lower = map(lambda x: x.lower(), samples.columns.tolist())
    samples.columns = colnames_lower

    datafile_column_names = samples.filter(regex="data_file\d+$").columns.tolist()

    samples_final = pandas.DataFrame()

    for col in datafile_column_names:
        samples_tmp = samples[SAMPLE_COLUMNS + [col, '%s_type' % col]]

        samples_tmp.rename(columns={col: 'data_file', '%s_type' % col: 'fileFormat'},
                           inplace=True)

        samples_final = pandas.concat([samples_final, samples_tmp], ignore_index=True)

    samples_final.filter(~samples_final.data_file.isnull())

    return samples_final

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

    # df.drop(["organism"], axis=1, inplace=True)

    # df = df[SAMPLE_COLUMNS]

    return df

def get_subjects(auth, guid):
    """Use the NDA API to get the `genomics_subject02` records for this GUID."""

    r = requests.get("https://ndar.nih.gov/api/guid/{}/data?short_name=genomics_subject02".format(guid),
                     auth=auth, headers={'Accept': 'application/json'})

    subject_guid_data = json.loads(r.text)

    tmp = []

    for row in subject_guid_data['age'][0]['dataStructureRow']:
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

    df = df.assign(sex=df.gender.replace(['M', 'F'], ['male', 'female']),
                   subject_sample_id_original=df.sample_id_original,
                   subject_biorepository=df.biorepository)

    df.drop(["gender", "sample_id_original", "biorepository"], axis=1, inplace=True)

    df = df.drop_duplicates()

    # df = df[SUBJECT_COLUMNS]

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

    df = df.assign(sex=df.gender.replace(['M', 'F'], ['male', 'female']))

    df.drop(["gender"], axis=1, inplace=True)

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
        tmp = pandas.read_csv(manifest_body, delimiter="\t", header=None)
        tmp.columns = ('filename', 'md5', 'size')
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
