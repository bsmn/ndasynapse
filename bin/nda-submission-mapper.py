import json
from io import StringIO
import pandas
import requests


# class ApplicationProperties:

#     def __init__(self, config_file):
#         self.config = json.load(file(config_file))['nda']

#     @property
#     def get_config(self):
#         return self.config


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

        print(self.submissions)
        
        self.submission_files = self.get_submission_files()

    def get_submissions_for_collection(self):

        request = requests.get(
            self.submission_api,
            params={'collectionId': self.collection_id,
                    'usersOwnSubmissions': False},
            headers=self.headers,
            auth=self.auth
        )
        try:
            submissions = json.loads(request.text)
            
        except json.decoder.JSONDecodeError:
            print('Error occurred retrieving submissions from collection {}'.format(self.collection_id))
            print('Request ({}) returned {}'.format(request.url, request.text))
        return [s['submission_id'] for s in submissions]

    def get_submission_files(self):
        submission_files = []
        for s in self.submissions:
            request = requests.get(
                self.submission_api + '/{}'.format(s),
                headers=self.headers,
                auth=self.auth
            )
            try:
                collection_id = json.loads(request.text)['collection']['id']
            except json.decoder.JSONDecodeError:
                print('Error occurred retrieving submission {}'.format(s))
                print('Request ({}) returned {}'.format(request.url, request.text))

            files = []
            request = requests.get(
                self.submission_api + '/{}/files'.format(s),
                headers=self.headers,
                auth=self.auth
            )
            try:
                files = json.loads(request.text)
            except json.decoder.JSONDecodeError:
                print('Error occurred retrieving files from submission {}'.format(s))
                print('Request returned {}'.format(request.text))
            submission_files.append({'files': NDASubmissionFiles(config, files),
                                     'collection_id': collection_id,
                                     'submission_id': s})
        return submission_files


if __name__ == "__main__":
    config = json.load(open("/home/kdaily/ndalogs_config.json"))['nda']
    submissions = NDASubmission(config=config, collection_id=2963)
    for submission in submissions.submission_files:
        print('GUIDs from submission {} in collection {}'.
              format(submission['submission_id'],
                     submission['collection_id']))
        for data_file in submission['files'].data_files:
            data_file_as_string = data_file['content'].decode('utf-8')
            if 'genomics_subject' in data_file_as_string:
                subject_data = pandas.read_csv(StringIO(data_file_as_string), skiprows=[1])
            if 'genomics_sample' in data_file_as_string:
                sample_data = pandas.read_csv(StringIO(data_file_as_string), skiprows=[1])
            if 'nichd_btb' in data_file_as_string:
                nichd_data = pandas.read_csv(StringIO(data_file_as_string), skiprows=[1])
        associated_files = pandas.DataFrame.from_dict(submission['files'].associated_files)
        print(associated_files)
        print(subject_data)
        print(sample_data)
        print(nichd_data)
