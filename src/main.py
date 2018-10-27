from botocore.client import ClientError

import boto3
import os
import json
import hashlib


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


class SyncDir:
    def __init__(self,
                 bucket_name=None,
                 start_dir=None,
                 state_path='./state_storage.json'):
        self.state_path = state_path
        self.start_dir = DIR
        self.bucket_name = bucket_name

        self.directory_state = self.load_previous_state(state_path)
        self.visited = {path: False for path in self.directory_state['paths']}

        if bucket_name and self.directory_state['bucket'] != bucket_name:
            self.directory_state['bucket'] = bucket_name
            print('New bucket detected!')
            print('Uploading files to new bucket.')
            print('Files in old bucket will still exist.')
            self.clear_cache()
        else:
            self.bucket_name = self.directory_state['bucket']

        if start_dir and self.directory_state['head_dir'] != start_dir:
            pass

        # To count the number of files sent over network
        self.transfer_counter = 0
        self.unchanged_files = 0

    def clear_cache(self):
        for path in self.directory_state['paths']:
            self.directory_state['paths'][path] = "RESET"

    def clear_bucket(self):
        for path in list(self.directory_state['paths'].keys()):
            self.delete_file_from_bucket(path)
        print('Backup deleted')

    def create_bucket_if_not_exists(self, bucket_name):
        if not s3.Bucket(bucket_name) in s3.buckets.all():
            s3.Bucket(bucket_name).create(CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
            })

    def delete_file_from_bucket(self, path):
        print('Deleting file from bucket :: {}'.format(path))
        del self.directory_state['paths'][path]
        s3_client.delete_object(Bucket=self.bucket_name, Key=path)

    def save_file_to_bucket(self, file_name):
        self.visited[file_name] = True

        file_hash = hash_file(file_name)
        if file_name not in self.directory_state['paths'] \
                or file_hash != self.directory_state['paths'][file_name]:

            self.directory_state['paths'][file_name] = file_hash
            self.transfer_counter += 1

            print('Uploading :: {}'.format(file_name))
            s3_client.upload_file(file_name, BUCKET_NAME, file_name)
        else:
            self.unchanged_files += 1

    def validate_cache(self):
        print('Checking status of sync...')
        valid = True
        valid_visited = {path: False for path in self.directory_state['paths']}

        for obj in s3.Bucket(self.bucket_name).objects.all():
            meta = s3_client.head_object(Bucket=self.bucket_name, Key=obj.key)
            bucket_md5 = meta['ETag'][1:-1]
            if obj.key not in self.directory_state['paths']:
                print('Found unknown file in Bucket :: {}'.format(obj.key))
                valid = False
                continue

            elif bucket_md5 != self.directory_state['paths'][obj.key]:
                print('File state corrupted in bucket! :: {}'.format(obj.key))
                print('    ...md5 checksum on bucket differs from local sum')
                valid = False

            valid_visited[obj.key] = True
        for path in valid_visited:
            if not valid_visited[path]:
                print('!!! File missing from bucket! :: {}'.format(path))
                print('!!!   To fix this, please rerun program.')
                self.directory_state['paths'][path] = 'RESET'
                valid = False

        print()
        if valid:
            print('All files are valid after sync!')
        else:
            print('Bucket state is invalid after sync...')
            print('See above errors for details')

    def check_for_deleted(self):
        deleted_counter = 0
        for path in self.visited:
            if not self.visited[path]:
                print('Found deleted file :: {}'.format(path))
                deleted_counter += 1
        if deleted_counter:
            print('Found {} files deleted locally'.format(deleted_counter))
            print()

    def load_previous_state(self, state_path):
        if not os.path.isfile(state_path):
            if self.start_dir and self.bucket_name and self.state_path:
                return {'bucket': self.bucket_name,
                        'head_dir': self.start_dir,
                        'paths': {}}
            else:
                print('No state file found!\n')
                print('Please run "python3 main.py -h" for setup instructions')
                exit(-1)
        return load_json_from_file(state_path)

    def save_current_state(self):
        serialize_json_to_file(self.state_path, self.directory_state)

    def main(self):
        self.create_bucket_if_not_exists(self.bucket_name)

        # self.clear_cache()
        recurse_file_structure(DIR, '/', self.save_file_to_bucket)

        self.check_for_deleted()
        # self.clear_bucket()

        print('{} new or modified files transfered'
              .format(self.transfer_counter))
        print('{} unmodified files'
              .format(self.unchanged_files))

        print()
        self.validate_cache()

        self.save_current_state()

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
        sync = SyncDir()
        # sync = SyncDir(BUCKET_NAME, DIR, JSON_DUMP)
        sync.main()
    except ClientError:
        print('Ooops! There has been a Boto3 ClientError...')
        print('\n  Are the AWS credentials properly configured?')
        print('  Is the bucket name valid?')
    except IOError:
        print('Ooops! There has been an IOError...')
        print('\n  Are you backing up a valid directory?')
        print('  Is the state being saved to a writeable path?')
