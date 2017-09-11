import io
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
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# ch.setFormatter(formatter)
# logger.addHandler(ch)

METADATA_COLUMNS = ['src_subject_id', 'experiment_id', 'subjectkey', 'sample_id_original',
                    'sample_id_biorepository', 'subject_sample_id_original', 'biorepository',
                    'subject_biorepository', 'sample_description', 'species', 'site', 'sex',
                    'sample_amount', 'phenotype', 'comments_misc', 'sample_unit', 'fileFormat']

SAMPLE_COLUMNS = ['datasetid', 'experiment_id', 'sample_id_original',
                  'sample_id_biorepository', 'organism', 'sample_amount', 'sample_unit',
                  'biorepository', 'comments_misc', 'site', 'genomics_sample03_id',
                  'src_subject_id', 'subjectkey']

SUBJECT_COLUMNS = ['src_subject_id', 'subjectkey', 'gender', 'race', 'phenotype',
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

MANIFEST_COLUMNS = ['filename', 'md5', 'size']

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
    samples['datasetId'] = map(lambda x: x['datasetId'], guid_data['age'][0]['dataStructureRow'])

    colnames_lower = map(lambda x: x.lower(), samples.columns.tolist())
    samples.columns = colnames_lower

    datafile_column_names = samples.filter(regex="data_file\d+$").columns.tolist()

    samples_final = pandas.DataFrame()

    for col in datafile_column_names:
        samples_tmp = samples[SAMPLE_COLUMNS + [col, '%s_type' % col, '%s_md5sum' % col, '%s_size' % col]]

        samples_tmp.rename(columns={col: 'data_file',
                                    '%s_type' % col: 'fileFormat',
                                    '%s_md5sum' % col: 'md5',
                                    '%s_size' % col: 'size'},
                           inplace=True)

        samples_final = pandas.concat([samples_final, samples_tmp], ignore_index=True)

    missing_data_file = samples_final.data_file.isnull()

    logger.info("These datasets are missing a data file and will be dropped: %s" % (samples_final.datasetid[missing_data_file].drop_duplicates().tolist(),))
    samples_final = samples_final[~missing_data_file]

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

def flattenjson( b, delim ):
    val = {}
    for i in b.keys():
        if isinstance( b[i], dict ):
            get = flattenjson( b[i], delim )
            for j in get.keys():
                val[ i + delim + j ] = get[j]
        else:
            val[i] = b[i]

    return val

def get_experiments(auth, experiment_ids, verbose=False):
    df = pandas.DataFrame()

    for experiment_id in experiment_ids:

        url = "https://ndar.nih.gov/api/experiment/{}".format(experiment_id)
        r = requests.get(url, auth=auth, headers={'Accept': 'application/json'})

        if verbose:
            logger.debug("Retrieved {}".format(url))

        guid_data = json.loads(r.text)
        guid_data_flat = flattenjson(guid_data[u'omicsOrFMRIOrEEG']['sections'], '.')

        fix_keys = ['processing.processingKits.processingKit',
                   'additionalinformation.equipment.equipmentName',
                   'extraction.extractionKits.extractionKit',
                    'additionalinformation.analysisSoftware.software']

        for key in fix_keys:
            foo = guid_data_flat[key]
            tmp = ",".join(map(lambda x: "%s %s" % (x['vendorName'], x['value']), foo))
            guid_data_flat[key] = tmp

        foo = guid_data_flat['processing.processingProtocols.processingProtocol']
        tmp = ",".join(map(lambda x: "%s: %s" % (x['technologyName'], x['value']), foo))
        guid_data_flat['processing.processingProtocols.processingProtocol'] = tmp

        guid_data_flat['extraction.extractionProtocols.protocolName'] = ",".join(
            guid_data_flat['extraction.extractionProtocols.protocolName'])

        guid_data_flat['experiment_id'] = experiment_id

        df = df.append(guid_data_flat, ignore_index=True)

    return df

def process_experiments(df):
    df_change = df[EXPERIMENT_COLUMNS_CHANGE.keys()]
    df_change = df_change.rename(columns=EXPERIMENT_COLUMNS_CHANGE, inplace=False)
    df2 = pandas.concat([df, df_change], axis=1)
    df2 = df2.rename(columns = lambda x: x.replace(".", "_"))
    df2['platform'] = df2['equipmentName'].replace({'Illumina HiSeq 2500,Illumina NextSeq 500': 'HiSeq2500,NextSeq500',
                                                    'Illumina NextSeq 500,Illumina HiSeq 2500': 'HiSeq2500,NextSeq500',
                                                    'Illumina HiSeq 4000,Illumina MiSeq': 'HiSeq4000,MiSeq',
                                                    'Illumina MiSeq,Illumina HiSeq 4000': 'HiSeq4000,MiSeq',
                                                    'Illumina NextSeq 500': 'NextSeq500',
                                                    'Illumina HiSeq 2500': 'HiSeq2500',
                                                    'Illumina HiSeq X Ten': 'HiSeqX',
                                                    'Illumina HiSeq 4000': 'HiSeq4000',
                                                    'Illumina MiSeq': 'MiSeq',
                                                    'BioNano IrysView': 'BionanoIrys'},
                                                  inplace=False)

    df2['assay'] = df2['applicationSubType'].replace({"Whole genome sequencing": "wholeGenomeSeq",
                                                      "Exome sequencing": "exomeSeq",
                                                      "Optical genome imaging": "wholeGenomeOpticalImaging",
                                                      }, inplace=False)

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
