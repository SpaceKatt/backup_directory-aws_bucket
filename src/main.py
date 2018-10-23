from botocore.client import ClientError

import boto3
import os
import json
import hashlib


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


class SyncDir:
    def __init__(self, bucket_name, start_dir, json_state):
        self.state_path = json_state
        self.directory_state = self.load_previous_state(json_state)
        self.bucket_name = bucket_name
        self.start_dir = DIR

        # To count the number of files sent over network
        self.transfer_counter = 0
        self.unchanged_files = 0

    def create_bucket_if_not_exists(self, bucket_name):
        if not s3.Bucket(bucket_name) in s3.buckets.all():
            s3.Bucket(bucket_name).create(CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
            })

    def save_file_to_bucket(self, file_name):
        file_hash = hash_file(file_name)
        if file_name not in self.directory_state \
                or file_hash != self.directory_state[file_name]:

            self.directory_state[file_name] = file_hash
            self.transfer_counter += 1

            print('Uploading :: {}'.format(file_name))
            s3_client.upload_file(file_name, BUCKET_NAME, file_name)
        else:
            self.unchanged_files += 1

    def load_previous_state(self, state_path):
        if not os.path.isfile(state_path):
            return {}
        return load_json_from_file(state_path)

    def save_current_state(self):
        serialize_json_to_file(self.state_path, self.directory_state)

    def main(self):
        self.create_bucket_if_not_exists(self.bucket_name)

        recurse_file_structure(DIR, '/', self.save_file_to_bucket)
        self.save_current_state()

        print('{} new or modified files transfered'
              .format(self.transfer_counter))
        print('{} unmodified files'
              .format(self.unchanged_files))

        self.transfer_counter = 0
        self.unchanged_files = 0


def serialize_json_to_file(json_path, json_obj):
    with open(json_path, 'w') as outfile:
        json.dump(json_obj, outfile)


def load_json_from_file(json_path):
    with open(json_path, 'r') as json_file:
        return json.load(json_file)


def recurse_file_structure(directory, dir_path, funct):
    for dir_name, sub_dir_list, file_list in os.walk(directory):
        for found_file in file_list:
            # Don't load these types of files
            if '.git' in dir_name or '.env' in dir_name:
                continue
            full_path = os.path.join(dir_name, found_file)

            funct(full_path)


def hash_file(file_path):
    md5 = hashlib.md5()

    with open(file_path, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            md5.update(data)
    return '{}'.format(md5.hexdigest())


if __name__ == '__main__':
    BUCKET_NAME = 'reee-bucket'
    DIR = '/home/spacekatt/projects/aiohttp_playground/'
    JSON_DUMP = 'state_storage.json'

    try:
        sync = SyncDir(BUCKET_NAME, DIR, JSON_DUMP)
        sync.main()
    except ClientError:
        print('Ooops! There has been a Boto3 ClientError...')
        print('\n  Are the AWS credentials properly configured?')
        print('  Is the bucket name valid?')
    except IOError:
        print('Ooops! There has been an IOError...')
        print('\n  Are you backing up a valid directory?')
        print('  Is the state being saved to a writeable path?')
