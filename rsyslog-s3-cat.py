#!/usr/bin/env python

import argparse
import os
import sys

from boto.s3.connection import S3Connection
from boto.s3.key import Key

def main():
	default_bucket = os.environ.get("S3_SYSLOG_BUCKET")
	default_prefix = None if os.environ.get("S3_SYSLOG_KEY_FORMAT") else "host/"

	parser = argparse.ArgumentParser()
	parser.add_argument("--bucket", type=str, default=default_bucket, help='default: "%s"' % default_bucket if default_bucket else "must be specified")
	parser.add_argument("--prefix", type=str, default=default_prefix, help='default: "%s"' % default_prefix if default_prefix else "must be specified")
	args = parser.parse_args()

	if not args.bucket:
		parser.print_help()
		sys.exit(1)

	conn = S3Connection(os.environ["S3_SYSLOG_ACCESS_KEY"], os.environ["S3_SYSLOG_SECRET_KEY"])
	bucket = conn.get_bucket(args.bucket)

	for key in bucket.list(args.prefix or ""):
		if not key.name[-1] == "/":
			sys.stdout.write(key.get_contents_as_string())

if __name__ == "__main__":
	main()
