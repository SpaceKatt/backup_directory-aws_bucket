# Directory Backup in an AWS S3 Bucket

Simple Python utility to backup a directory, and all of its contents 
(recursively), into an S3 bucket.

# Prerequisites

This project was developed and tested with Python 3.5.2 on Linux. The user
is also expected to have an AWS account, with their client Id and secret handy.

# Setup

The user must first configure their AWS CLI, so `boto3` recognizes the user
and their credentials are stored in `~/.aws/credentials`:

  - `aws configure`

Install dependencies from `requirements.txt`:

  - `python3 -m pip install -r requirements.txt`


# Usage

1. Perform a first time setup so local cache knows what directory to backup
   and which bucket to send backup to

  - `python3 main.py -d <DIRECTORY> -b <BUCKET_NAME>`

2. Future backups may be performed by simply running the main script

  - `python3 main.py`
