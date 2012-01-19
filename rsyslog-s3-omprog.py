#!/usr/bin/python -u

import os
import Queue as queuelib
import socket
import sys
import threading
import time

from boto.s3.connection import S3Connection
from boto.s3.key import Key

def main():
	queue = queuelib.Queue(1)
	receive = Receive(queue)
	storage = Storage(queue)

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

	max_delay = 10 * 60
	max_lines = 10000

	default_path_format = "host/%Y-%m-%d/{hostname}/%H:%M:%S"

	def __init__(self, queue):
		conn = S3Connection(os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"])
		self.bucket = conn.get_bucket(os.environ["S3_BUCKET"])
		self.path_format = os.environ.get("S3_PATH_FORMAT", self.default_path_format)
		self.params = dict(hostname=socket.gethostname())
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
		key.key = time.strftime(self.path_format).format(**self.params)
		key.set_contents_from_string("".join(log))

		del log[:]

if __name__ == "__main__":
	main()
