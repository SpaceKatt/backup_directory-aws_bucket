from botocore.client import ClientError

import argparse
import boto3
import os
import json
import hashlib
import sys
import traceback


s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


class SyncDir:
    def __init__(self,
                 bucket_name=None,
                 start_dir=None,
                 state_path='./state_storage.json',
                 validate=True,
                 obviate_cache=False,
                 remove_old=False,
                 delete=False,
                 only_delete=False):

        self.state_path = os.path.abspath(state_path)
        self.bucket_name = bucket_name
        self.start_dir = start_dir
        if self.start_dir:
            if not os.path.exists(start_dir):
                print('!!! Directory to backup does not exist!')
                print('    Please try again!')
                sys.exit(-1)
        self.validate = validate

        self.obviate_cache = obviate_cache
        self.remove_old = remove_old
        self.delete_bucket = delete
        self.only_delete = only_delete

        self.directory_state = self.load_previous_state(state_path)
        self.visited = {path: False for path in self.directory_state['paths']}

        if bucket_name and self.directory_state['bucket'] != bucket_name:
            self.directory_state['bucket'] = bucket_name
            print('New bucket detected!')
            print('Uploading files to new bucket.')
            print('Files in old bucket will still exist.\n')
            self.clear_cache()
        elif not bucket_name:
            self.bucket_name = self.directory_state['bucket']

        if self.start_dir:
            self.start_dir = os.path.abspath(self.start_dir)
            if self.directory_state['head_dir'] != self.start_dir:
                print('Uploading new directory will delete bucket contents...')
                answer = query_yes_no('Do you wish to delete old files?')
                print()
                if answer:
                    self.delete_bucket = True
                    self.start_dir = os.path.abspath(start_dir)
                    self.directory_state['head_dir'] = self.start_dir
                else:
                    print('Goodbye, World!')
                    sys.exit(0)
        else:
            self.start_dir = self.directory_state['head_dir']

        # To count the number of files sent over network
        self.transfer_counter = 0
        self.unchanged_files = 0

    def clear_cache(self):
        print('Clearing contents of local cache...')
        for path in list(self.directory_state['paths']):
            del self.directory_state['paths'][path]
        self.visited = {}
        print('Cache cleared!\n')

    def clear_bucket(self):
        print('Clearing contents of S3 bucket...')
        for path in list(self.directory_state['paths'].keys()):
            self.delete_file_from_bucket(path)
        print('Bucket cleared\n')

    def create_bucket_if_not_exists(self, bucket_name):
        try:
            if not s3.Bucket(bucket_name) in s3.buckets.all():
                print('Bucket "{}" not found, creating new bucket...'
                      .format(bucket_name))
                self.obviate_cache = True
                print('Clearing cache for new bucket\n')
                s3.Bucket(bucket_name).create(CreateBucketConfiguration={
                    'LocationConstraint': 'us-west-2'
                })
        except ClientError:
            print('!!! Error connecting to bucket...')
            print('!!! Are you using a unique bucket name that you own?')
            print('!!! Are your AWS credentials configured?')
            print()
            print('Please try a new bucket name. For more, please use --help')
            sys.exit(-1)

    def delete_file_from_bucket(self, path):
        print('Deleting file from bucket :: {}'.format(path))
        del self.directory_state['paths'][path]
        s3_client.delete_object(Bucket=self.bucket_name, Key=path)

    def save_file_to_bucket(self, file_name):
        self.visited[file_name] = True

        file_name = os.path.abspath(file_name)
        if file_name == os.path.abspath(self.state_path):
            print('Skipping local cache file :: {}'.format(self.state_path))
            return

        file_hash = hash_file(file_name)
        if file_name not in self.directory_state['paths'] \
                or file_hash != self.directory_state['paths'][file_name]:

            self.directory_state['paths'][file_name] = file_hash
            self.transfer_counter += 1

            print('Uploading :: {}'.format(file_name))
            s3_client.upload_file(file_name, self.bucket_name, file_name)
        else:
            self.unchanged_files += 1

    def validate_cache(self):
        print('Checking status of sync...')
        print('If this takes too long, this step can be skipped with "-t"\n')
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
                self.directory_state['paths'][obj.key] = bucket_md5
                print('  To fix this error, please rerun program')
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
                print('Found locally deleted file :: {}'.format(path))
                deleted_counter += 1
                if self.remove_old:
                    self.delete_file_from_bucket(path)
        if deleted_counter:
            if self.remove_old:
                print('{} files deleted from backup'.format(deleted_counter))
            else:
                print('Found {} files deleted locally'.format(deleted_counter))
                print('To remove from backup, run again with the "-s" flag')
            print()

    def load_previous_state(self, state_path):
        if not os.path.isfile(state_path):
            if self.start_dir and self.bucket_name and self.state_path:
                return {'bucket': self.bucket_name,
                        'head_dir': self.start_dir,
                        'paths': {}}
            else:
                print('!!! No state file found and not enough information !!!')
                print('!!! Please specify both --directory and --bucket   !!!')
                print()
                print('Please see "python3 main.py --help"')
                exit(-1)
        state = load_json_from_file(state_path)
        return state

    def save_current_state(self):
        serialize_json_to_file(self.state_path, self.directory_state)

    def main(self):
        self.create_bucket_if_not_exists(self.bucket_name)

        if self.delete_bucket or self.only_delete:
            self.clear_bucket()
            self.clear_cache()

            if self.only_delete:
                self.save_current_state()
                print('Success deleting files from Bucket!')
                sys.exit(0)

        # if delete_bucket, then cache is already cleared
        if not self.delete_bucket and self.obviate_cache:
            self.clear_cache()

        # Save any file to the backup that hasn't been uploaded
        print('Syncing files to bucket :: "{}"\n'.format(self.bucket_name))
        recurse_file_structure(self.start_dir, '/', self.save_file_to_bucket)

        self.check_for_deleted()

        print('{} new or modified files transfered'
              .format(self.transfer_counter))
        print('{} unmodified files'
              .format(self.unchanged_files))

        if self.validate:
            print()
            self.validate_cache()

        # Save local cache
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
            # Don't upload git configuration and history
            if '.git' in dir_name:
                continue

            if '.env' in found_file:
                print('Skipping sensitive file :: {}'.format(found_file))
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


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".

    Function code from public domain ::
        http://code.activestate.com/recipes/577058/
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='''
        Backs up a directory to an AWS S3 bucket of your choosing. Prior
        to using this service, please configure your AWS credentials by
        logging in via the AWS CLI tool, using "aws configure". The first time
        this service is used, please use the --directory and --bucket arguments
        to setup which directory to backup and AWS S3 bucket to use. After
        the first time setup, a local cache will be created to persist this
        information.
        ''')
    parser.add_argument('-d', '--directory', default=None,
                        help='The directory you wish to backup')
    parser.add_argument('-b', '--bucket', default=None,
                        help='The name of the bucket we are backing up in')
    parser.add_argument('-p', '--state_path', default='state_storage.json',
                        help='Custom path of local cache of previous state')

    parser.add_argument('-t', '--trust', action='store_false',
                        help='Trust state after sync and don\'t verify it')
    parser.add_argument('-s', '--strict', action="store_true",
                        help='Remove deleted files that still exist in backup')
    parser.add_argument('-i', '--ignore_cache', action="store_true",
                        help='Ignore local state and upload every file found')

    parser.add_argument('-k', '--kill', action="store_true",
                        help='Delete files in bucket before uploading')
    parser.add_argument('-x', '--expunge', action="store_true",
                        help='Remove everything from backup without upload')

    args = parser.parse_args()

    BUCKET_NAME = args.bucket
    DIR = args.directory
    JSON_DUMP = args.state_path
    VALIDATE = args.trust
    REMOVE_OLD = args.strict
    OBVIATE_CACHE = args.ignore_cache
    DELETE = args.kill
    ONLY_DELETE = args.expunge

    try:
        sync = SyncDir(bucket_name=BUCKET_NAME,
                       start_dir=DIR,
                       state_path=JSON_DUMP,
                       validate=VALIDATE,
                       obviate_cache=OBVIATE_CACHE,
                       remove_old=REMOVE_OLD,
                       delete=DELETE,
                       only_delete=ONLY_DELETE)
        sync.main()
    except ClientError:
        print('Ooops! There has been a Boto3 ClientError...')
        print('\n  Are the AWS credentials properly configured?')
        print('  Is the bucket name valid?')
    except IOError:
        print('Ooops! There has been an IOError...')
        print('\n  Are you backing up a valid directory?')
        print('  Is the state being saved to a writeable path?')
    except Exception:
        print('Network error! Please retry...')
        print('If this keeps happening, then please submit a bug report :)')
        traceback.print_exec()
