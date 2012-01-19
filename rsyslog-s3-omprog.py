#!/usr/bin/python -u

import Queue as queuelib
import argparse
import os
import socket
import sys
import threading
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

default_delay = 10 * 60
default_lines = 10000

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("--delay", type=int, default=default_delay, help="in seconds")
	parser.add_argument("--lines", type=int, default=default_lines)
	args = parser.parse_args()

	queue = queuelib.Queue(1)
	receive = Receive(queue)
	storage = Storage(queue, args.delay, args.lines)

	thread = threading.Thread(target=receive.loop)
	thread.daemon = True
	thread.start()

	storage.loop()

class Receive(object):

	def __init__(self, queue):
		self.queue = queue

	def loop(self):
		try:
			while True:
				line = sys.stdin.readline()
				if not line:
					break

				self.queue.put(line)
		finally:
			self.queue.put(StopIteration)

class Storage(object):

	key_format = "host/%Y-%m-%d/{hostname}/%H:%M:%S"

	def __init__(self, queue, max_delay, max_lines):
		conn = S3Connection(os.environ["S3_SYSLOG_ACCESS_KEY"], os.environ["S3_SYSLOG_SECRET_KEY"])
		self.bucket = conn.get_bucket(os.environ["S3_SYSLOG_BUCKET"])

		self.key_format = os.environ.get("S3_SYSLOG_KEY_FORMAT", self.key_format)
		self.key_params = dict(hostname=socket.gethostname())

		self.max_delay = max_delay
		self.max_lines = max_lines

		self.queue = queue

	def loop(self):
		last_time = time.time()
		log = []

		try:
			while True:
				curr_time = time.time()

				timeout = self.max_delay - (curr_time - last_time)
				if timeout <= 0:
					self.flush(log)
					last_time = curr_time

					timeout = None

				line = self.queue.get(True, timeout)
				if line is StopIteration:
					break

				log.append(line)
				if len(log) >= self.max_lines:
					if int(curr_time) == int(last_time):
						time.sleep(1)
						curr_time = time.time()

					self.flush(log)
					last_time = curr_time
		finally:
			self.flush(log)

	def flush(self, log):
		if not log:
			return

		key = Key(self.bucket)
		key.key = time.strftime(self.key_format).format(**self.key_params)
		key.set_contents_from_string("".join(log))

		del log[:]

if __name__ == "__main__":
	main()
