import json
import requests
from unittest.mock import Mock, patch

from nose.tools import assert_is_not_none, assert_list_equal
import ndasynapse

_submission_data_example = json.loads('''{
  "_links": {
      "self": {
        "href": "https://nda.nih.gov/api/submission/12345"
      }
  },
  "collection": {
    "id": "1234",
    "title": "Collection 1234"
  },
  "dataset_created_date": "2019-09-03T14:14:24.006-0400",
  "dataset_modified_date": null,
  "dataset_description": "My Submission for Package Tests",
  "dataset_title": "My Submission",
  "submission_id": "12345",
  "submission_status": "Upload Completed"
}''')

_guid_data_genomics_subject02_example = json.loads('''
  {
    "guid": "NDAR_XXXXXXXXXXX",
    "currentGUID": "NDAR_XXXXXXXXXXX",
    "age": [
      {
        "value": 999,
        "dataStructureRow": [
          {
            "links": {
              "link": [
                {
                  "value": "",
                  "rel": "data_structure",
                  "href": "https://ndar.nih.gov/api/datadictionary/v2/datastructure/genomics_subject02",
                  "md5sum": null,
                  "size": null
                },
                {
                  "value": "",
                  "rel": "collection",
                  "href": "https://ndar.nih.gov/edit_collection.html?id=2458",
                  "md5sum": null,
                  "size": null
                }
              ]
            },
            "shortName": "genomics_subject02",
            "rowNumber": 92027,
            "datasetId": 10000,
            "dataElement": [
              {
                "value": "99999",
                "name": "GENOMICS_SUBJECT02_ID",
                "md5sum": null,
                "size": null
              },
              {
                "value": "NDAR_XXXXXXXXXXX",
                "name": "SUBJECTKEY",
                "md5sum": null,
                "size": null
              },
              {
                "value": "1111",
                "name": "SRC_SUBJECT_ID",
                "md5sum": null,
                "size": null
              },
              {
                "value": "M",
                "name": "SEX",
                "md5sum": null,
                "size": null
              },
              {
                "value": "White",
                "name": "RACE",
                "md5sum": null,
                "size": null
              },
              {
                "value": "normal",
                "name": "PHENOTYPE",
                "md5sum": null,
                "size": null
              },
              {
                "value": "Multiple injuries",
                "name": "PHENOTYPE_DESCRIPTION",
                "md5sum": null,
                "size": null
              },
              {
                "value": "No",
                "name": "TWINS_STUDY",
                "md5sum": null,
                "size": null
              },
              {
                "value": "No",
                "name": "SIBLING_STUDY",
                "md5sum": null,
                "size": null
              },
              {
                "value": "No",
                "name": "FAMILY_STUDY",
                "md5sum": null,
                "size": null
              },
              {
                "value": "Yes",
                "name": "SAMPLE_TAKEN",
                "md5sum": null,
                "size": null
              },
              {
                "value": "1111",
                "name": "SAMPLE_ID_ORIGINAL",
                "md5sum": null,
                "size": null
              },
              {
                "value": "brain",
                "name": "SAMPLE_DESCRIPTION",
                "md5sum": null,
                "size": null
              },
              {
                "value": "Some Biorepository",
                "name": "BIOREPOSITORY",
                "md5sum": null,
                "size": null
              },
              {
                "value": "1111",
                "name": "PATIENT_ID_BIOREPOSITORY",
                "md5sum": null,
                "size": null
              },
              {
                "value": "1111",
                "name": "SAMPLE_ID_BIOREPOSITORY",
                "md5sum": null,
                "size": null
              }
            ]
          }]
    }]
  }
  ''')

_guid_data_genomics_sample03_example = json.loads('''{
  "guid": "NDAR_XXXXXXXXXXX",
  "currentGUID": "NDAR_XXXXXXXXXXX",
  "age": [
    {
      "value": 999,
      "dataStructureRow": [
        {
          "links": {
            "link": [
              {
                "value": "",
                "rel": "experiment_id",
                "href": "https://ndar.nih.gov/experimentView.html?experimentId=123",
                "md5sum": null,
                "size": null
              },
              {
                "value": "",
                "rel": "data_structure",
                "href": "https://ndar.nih.gov/api/datadictionary/v2/datastructure/genomics_sample03",
                "md5sum": null,
                "size": null
              },
              {
                "value": "",
                "rel": "collection",
                "href": "https://ndar.nih.gov/edit_collection.html?id=2458",
                "md5sum": null,
                "size": null
              },
              {
                "value": "",
                "rel": "data_file",
                "href": "s3://NDAR_Central_3/submission_12345/file1.fastq.gz",
                "md5sum": "ce84da1a84aacc55cc50a98db17e9823",
                "size": "481114727"
              },
              {
                "value": "",
                "rel": "data_file",
                "href": "s3://NDAR_Central_3/submission_12345/file2.fastq.gz",
                "md5sum": "33070aafb06ebefd7d37d3a98d51cdd1",
                "size": "549375212"
              }
            ]
          },
          "shortName": "genomics_sample03",
          "rowNumber": 146319,
          "datasetId": 11111,
          "dataElement": [
            {
              "value": "123456",
              "name": "GENOMICS_SAMPLE03_ID",
              "md5sum": null,
              "size": null
            },
            {
              "value": "123",
              "name": "EXPERIMENT_ID",
              "md5sum": null,
              "size": null
            },
            {
              "value": "NDAR_XXXXXXXXXXX",
              "name": "SUBJECTKEY",
              "md5sum": null,
              "size": null
            },
            {
              "value": "1111",
              "name": "SRC_SUBJECT_ID",
              "md5sum": null,
              "size": null
            },
            {
              "value": "frontal cortex",
              "name": "SAMPLE_DESCRIPTION",
              "md5sum": null,
              "size": null
            },
            {
              "value": "sample1",
              "name": "SAMPLE_ID_ORIGINAL",
              "md5sum": null,
              "size": null
            },
            {
              "value": "Homo Sapiens",
              "name": "ORGANISM",
              "md5sum": null,
              "size": null
            },
            {
              "value": "20",
              "name": "SAMPLE_AMOUNT",
              "md5sum": null,
              "size": null
            },
            {
              "value": "ug - micrograms",
              "name": "SAMPLE_UNIT",
              "md5sum": null,
              "size": null
            },
            {
              "value": "MDA;WGS;QC",
              "name": "DATA_CODE",
              "md5sum": null,
              "size": null
            },
            {
              "value": "FASTQ",
              "name": "DATA_FILE1_TYPE",
              "md5sum": null,
              "size": null
            },
            {
              "value": "<![CDATA[s3://NDAR_Central_3/submission_12345/file1.fastq.gz]]>",
              "name": "DATA_FILE1",
              "md5sum": "ce84da1a84aacc55cc50a98db17e9823",
              "size": "481114727"
            },
            {
              "value": "FASTQ",
              "name": "DATA_FILE2_TYPE",
              "md5sum": null,
              "size": null
            },
            {
              "value": "<![CDATA[s3://NDAR_Central_3/submission_12345/file2.fastq.gz]]>",
              "name": "DATA_FILE2",
              "md5sum": "33070aafb06ebefd7d37d3a98d51cdd1",
              "size": "549375212"
            },
            {
              "value": "-80",
              "name": "STORAGE_PROTOCOL",
              "md5sum": null,
              "size": null
            },
            {
              "value": "NDAR",
              "name": "DATA_FILE_LOCATION",
              "md5sum": null,
              "size": null
            },
            {
              "value": "Some Biorepository",
              "name": "BIOREPOSITORY",
              "md5sum": null,
              "size": null
            },
            {
              "value": "1111",
              "name": "PATIENT_ID_BIOREPOSITORY",
              "md5sum": null,
              "size": null
            },
            {
              "value": "1111_NeuN_positive",
              "name": "SAMPLE_ID_BIOREPOSITORY",
              "md5sum": null,
              "size": null
            },
            {
              "value": "Some comments",
              "name": "COMMENTS_MISC",
              "md5sum": null,
              "size": null
            },
            {
              "value": "U0199999999",
              "name": "SITE",
              "md5sum": null,
              "size": null
            },
            {
              "value": "1",
              "name": "SEQ_BATCH",
              "md5sum": null,
              "size": null
            }
          ]
        }]}]}''')


@patch("ndasynapse.nda.requests.get")
def test_get_guid_data(mock_get):
    mock_get.return_value.ok = True
    response = ndasynapse.nda.get_guid_data(auth=None, subjectkey=None, 
                                            short_name=None)
    assert_is_not_none(response)


@patch('ndasynapse.nda.requests.get')
def test_get_guid_data_ok(mock_get):
    data = _guid_data_genomics_subject02_example

    # Configure the mock to return a response with an OK status code. Also, the mock should have
    # a `json()` method that returns a list of todos.
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.json.return_value = data

    # Call the service, which will send a request to the server.
    response = ndasynapse.nda.get_guid_data(auth=None, subjectkey=None, 
                                            short_name=None)

    # If the request is sent successfully, then I expect a response to be returned.
    assert_list_equal([response], [data])


@patch('ndasynapse.nda.requests.get')
def test_get_sample(mock_get):
    data = _guid_data_genomics_sample03_example

    # Configure the mock to return a response with an OK status code. Also, the mock should have
    # a `json()` method that returns a list of todos.
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.json.return_value = data

    # Call the service, which will send a request to the server.
    response = ndasynapse.nda.get_samples(auth=None, guid=None)

    # If the request is sent successfully, then I expect a response to be returned.
    assert_list_equal([response], [data])


@patch('ndasynapse.nda.requests.get')
def test_get_subject(mock_get):
    data = _guid_data_genomics_subject02_example

    # Configure the mock to return a response with an OK status code. Also, the mock should have
    # a `json()` method that returns a list of genomic subject data.
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.json.return_value = data

    # Call the service, which will send a request to the server.
    response = ndasynapse.nda.get_subjects(auth=None, guid=None)

    # If the request is sent successfully, then I expect a response to be returned.
    assert_list_equal([response], [data])


@patch('ndasynapse.nda.requests.get')
def test_get_submission(mock_get):
    data = _submission_data_example

    # Configure the mock to return a response with an OK status code. Also, the mock should have
    # a `json()` method that returns a list of genomic subject data.
    mock_get.return_value = Mock(ok=True)
    mock_get.return_value.json.return_value = data

    # Call the service, which will send a request to the server.
    response = ndasynapse.nda.get_submission(auth=None, submissionid=12345)

    # If the request is sent successfully, then I expect a response to be returned.
    assert_list_equal([response], [data])


def test_get_submission_ids():
    data = _guid_data_genomics_sample03_example

    row = data["age"][0]["dataStructureRow"][0]
    submission_ids = ndasynapse.nda.get_submission_ids_from_links(
      data_structure_row=row)
  
    assert submission_ids == set([12345])


def test_get_collection_ids():
    data = _guid_data_genomics_sample03_example

    row = data["age"][0]["dataStructureRow"][0]
    submission_ids = ndasynapse.nda.get_collection_ids_from_links(
        data_structure_row=row)

    assert submission_ids == set([2458])


def test_get_experiment_ids():
    data = _guid_data_genomics_sample03_example

    row = data["age"][0]["dataStructureRow"][0]
    submission_ids = ndasynapse.nda.get_experiment_ids_from_links(
        data_structure_row=row)
  
    assert submission_ids == set([123])
