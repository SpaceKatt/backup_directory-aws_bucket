# Directory Backup in an AWS S3 Bucket

Simple Python utility to backup a directory, and all of its contents 
(recursively), into an S3 bucket.

## Prerequisites

The user is expected to have an AWS account, along with their client Id
and secret.

This project was developed and tested with Python 3.5.2 on Linux.

## Setup

The user must first configure their AWS CLI, so `boto3` recognizes the user
and their credentials are stored in `~/.aws/credentials`:

  - `aws configure`

Install dependencies from `requirements.txt`:

  - `python3 -m pip install -r requirements.txt`


## Basic Usage

Note: Always run `main.py` from inside the `src/` directory.

1. Perform a first time setup so local cache knows what directory to backup
   and which bucket to send backup to

  - `python3 main.py -d <DIRECTORY> -b <BUCKET_NAME>`

2. Future backups may be performed by simply running the main script

  - `python3 main.py`

Overview of additional arguments:

| Argument | Description | Example |
| -------- | ----------- | ------- |
| -h, --help | show help message and exit | `python3 main.py --help` |
| -d DIRECTORY, --directory DIRECTORY | The directory you wish to backup | `python3 main.py -d /home/username/projects` |
| -b BUCKET, --bucket BUCKET | The name of the bucket we are backing up in | `python3 main.py -b foo-bucket` |
| -p STATE_PATH, --state_path STATE_PATH | Custom path of local cache of previous state | `python3 -p state_storage.json` |
| -t, --trust |  Trust state after sync and don't verify it | `python3 main.py -t` |
| -s, --strict |  Remove deleted files that still exist in backup | `python3 main.py -s` |
| -i, --ignore_cache |  Ignore local state and upload every file found | `python3 main.py -i` |
| -k, --kill |  Delete files in bucket before uploading | `python3 main.py -k` |
| -x, --expunge |  Remove everything from backup without upload | `python3 main.py -x` |

