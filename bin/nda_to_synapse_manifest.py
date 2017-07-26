import os
import json
import synapseclient
import pandas
import requests
import boto3
import nda_aws_token_generator
import logging

import ndasynapse

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
s3 = boto3.resource("s3")
obj = s3.Object('kdaily-lambda-creds.sagebase.org', 'ndalogs_config.json')

config = json.loads(obj.get()['Body'].read())

ndaconfig = config['nda']

s3_nda = ndasynapse.nda.get_nda_s3_session(ndaconfig['username'], ndaconfig['password'])

# Synapse
# Using the concatenated manifests as the master list of files to store, create file handles and entities in Synapse.
# Use the metadata table to get the appropriate tissue/subject/sample annotations to set on each File entity.

auth = requests.auth.HTTPBasicAuth(ndaconfig['username'], ndaconfig['password'])
bucket = s3_nda.Bucket(NDA_BUCKET_NAME)

samples = ndasynapse.nda.get_samples(auth, guid=REFERENCE_GUID)
samples = ndasynapse.nda.process_samples(samples)
samples.to_csv("./samples.csv")

subjects = ndasynapse.nda.get_subjects(auth, REFERENCE_GUID)
subjects = ndasynapse.nda.process_subjects(subjects)

subjects.to_csv("./subjects.csv")

btb = ndasynapse.nda.get_tissues(auth, REFERENCE_GUID)
btb = ndasynapse.nda.process_tissues(btb)
btb.to_csv('./btb.csv')

btb_subjects = ndasynapse.nda.merge_tissues_subjects(btb, subjects)
btb_subjects.to_csv('btb_subjects.csv')

manifest = ndasynapse.nda.get_manifests(bucket)
# Only keep the files that are in the metadata table
manifest = ndasynapse.nda.manifest[manifest.filename.isin(metadata.data_file)]
manifest.to_csv('./manifest.csv')

metadata_manifest = ndasynapse.nda.merge_metadata_manifest(metadata, manifest)
metadata_manifest.to_csv('./metadata_manifest.csv')

fh_list = ndasynapse.synapse.create_synapse_filehandles(metadata_manifest, NDA_BUCKET_NAME)

fh_ids = map(lambda x: x['id'], fh_list)
synapse_manifest = metadata_manifest[METADATA_COLUMNS]
synapse_manifest.dataFileHandleId = fh_ids
synapse_manifest.Path = None

# f_list = ndasynapse.synapse.store(synapse_manifest)
# a = a.to_dict()
