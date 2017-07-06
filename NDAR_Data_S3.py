
# coding: utf-8

# In[30]:

import os
import json
import synapseclient
import StringIO
import csv
import boto
import pandas
import json

def alias_column(df, column_name, alias_dict):
    """Alias a column of a Pandas DataFrame.
    
    At a minimum, copies a column to the aliased column name.
    
    If 'column_aliases' are specified for the column, then values in the new column are replaced.
    
    """
       
    alias = alias_dict['alias']
    
    try:
        column_from = alias_dict['column_aliases'].keys()
        column_to = alias_dict['column_aliases'].values()
        replace = True
    except KeyError:
        replace = False
    
    df[alias] = df[column_name]
    
    if replace:
        df[alias] = df[alias].replace(column_from, column_to)
        
    return df

def alias_columns(df, column_names, alias_dict):
    for column in column_names:
        try:
            df = alias_column(df, column, alias_dict[column])
        except Exception as e:
            print e
    
    return df

syn = synapseclient.login(silent=True)

con = boto.connect_s3(profile_name='sagestaticbucket')
con.get_bucket('nda-bsmn')
b = con.get_bucket('nda-bsmn')

base_path = 'abyzova_1481392262177'
synapse_data_folder = 'syn7872188'
synapse_data_folder_id = int(synapse_data_folder.replace('syn', ''))
storage_location_id = '9209'
bucket_name = 'nda-bsmn'

subject_file_name = 'BSMN_REF_subject.csv'
samples_file_name = 'BSMN_REF_samples.final.txt'
manifest_file_name = '.manifest'

# Set up aliases and lookup dictionaries
content_type_dict = {'.gz': 'application/x-gzip', '.bam': 'application/octet-stream'}
subject_column_aliases = json.load(file("subject_column_aliases.json"))
sample_column_aliases = json.load(file("sample_column_aliases.json"))

# Read files
subject_key = b.get_key("%s/%s" % (base_path, subject_file_name))
manifest_key = b.get_key("%s/%s" % (base_path, manifest_file_name))
samples_key = b.get_key("%s/%s" % (base_path, samples_file_name))

subject = pandas.read_csv(StringIO.StringIO(subject_key.read()), delimiter=",", header=1)
samples = pandas.read_csv(StringIO.StringIO(samples_key.read()), delimiter="\t", header=1)
manifest = pandas.read_csv(StringIO.StringIO(manifest_key.read()), delimiter="\t", names=['filename', 'md5', 'size'])


# In[31]:

### Process subject file
subject = alias_columns(subject, subject_column_aliases.keys(), subject_column_aliases)

subject = subject[['src_subject_id', 'gender', 'race', 'ethnic_group', 'phenotype', 
                   'subject_sample_id_original', 'sample_description', 'subject_biorepository', 'sex']]


# In[32]:

### Process sample file
samples1 = samples[['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original', 'organism', 
                   'sample_amount', 'sample_unit', 'biorepository', 'comments_misc', 'site', 'data_file1', 'data_file1_type']]

samples1 = alias_columns(samples1, sample_column_aliases.keys(), sample_column_aliases)

aliased_cols = list(set(samples1.columns).intersection([x['alias'] for x in sample_column_aliases.values()]))

keep_cols = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original', 'organism', 
             'sample_amount', 'sample_unit', 'biorepository', 'comments_misc', 'site'] + aliased_cols

samples1 = samples1[keep_cols]

samples2 = samples[['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original', 'organism',
                    'sample_amount', 'sample_unit', 'biorepository', 'comments_misc', 'site', 'data_file2', 'data_file2_type']]

samples2 = alias_columns(samples2, sample_column_aliases.keys(), sample_column_aliases)

aliased_cols = list(set(samples2.columns).intersection([x['alias'] for x in sample_column_aliases.values()]))

keep_cols = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original', 'organism', 
             'sample_amount', 'sample_unit', 'biorepository', 'comments_misc', 'site'] + aliased_cols

samples2 = samples2[keep_cols]

samples3 = pandas.concat([samples1, samples2], ignore_index=True)

# Remove initial slash to match what is in manifest file
samples3.filename = samples3['filename'].apply(lambda value: value[1:] if not pandas.isnull(value) else value)


# In[33]:

# Merge to make metadata

metadata = pandas.merge(samples3, subject, how='left', 
                        left_on='src_subject_id', right_on='src_subject_id')

metadata.index = metadata.filename

metadata.drop('filename', axis=1, inplace=True)

manifest_list = manifest.transpose().to_dict().values()


# In[36]:

# Make links in Synapse

force_remove = False

for (n, x) in enumerate(manifest_list):
    s3FilePath = os.path.basename("%s/%s" % (base_path, x['filename']))
    contentSize = x['size']
    contentMd5 = x['md5']

    # Check if it exists in Synapse
    res = syn.restGET("/entity/md5/%s" % (contentMd5, ))['results']
    
    res = filter(lambda x: x['benefactorId'] == synapse_data_folder_id, res)
    
    if len(res) > 0:
        print "%s already exists in Synapse (count = %s)" % (os.path.split(x['filename'])[1], len(res))

        if force_remove:

            for entity_record in res:
                fhs = syn.restGET("/entity/%(id)s/version/%(versionNumber)s/filehandles" % entity_record)
        
            syn.delete(entity_record['id'], version=entity_record['versionNumber'])
        
            for fh in fhs['list']:
                syn.restDELETE("/fileHandle/%(id)s" % fh, syn.fileHandleEndpoint)
    
            
    else:
        print "Adding %s (%s)" % (contentMd5, x['filename'])
        break

        
    contentType = content_type_dict.get(os.path.splitext(x['filename'])[-1],
                                        'application/octet-stream')
    
    try:
        a = metadata.loc[x['filename']].to_dict()
    except KeyError:
        a = {}
    
    fileHandle = {'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
                  'fileName'    : s3FileName,
                  'contentSize' : contentSize,
                  'contentType' : contentType,
                  'contentMd5' :  contentMd5,
                  'bucketName' : bucket_name,
                  'key'        : s3FilePath,
                  'storageLocationId' : storage_location_id}

    fileHandleObj = syn.restPOST('/externalFileHandle/s3', 
                                 json.dumps(fileHandle), 
                                 endpoint=syn.fileHandleEndpoint)

    f = synapseclient.File(parentId=synapse_data_folder, 
                           name=s3FileName, 
                           dataFileHandleId = fileHandleObj['id'])
    f.annotations = a

    f = syn.store(f, forceVersion=False)

