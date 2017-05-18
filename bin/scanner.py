#!/usr/bin/python3.5

###########################################
#                                         #
# DATAradical - Storage Insight - Scanner #
#                                         #
# © 2017. All rights reserved.            #
#                                         #
###########################################

programname = 'DATAradical Storage Insight Scanner'
programversion = 'v0.0'

import sys
from stat import *
import datetime
import socket
import logging

def is_directory(directory):
	"""Checks if a path is an actual directory"""
	if not os.path.isdir(directory):
		msg = "{0} is not a directory".format(directory)
		raise argparse.ArgumentTypeError(msg)
	else:
		return directory

def cli_arguments():
	import argparse
	import time
	parser = argparse.ArgumentParser(description=programname)
	parser.add_argument('-v', '--verbose', help='Increase verbosity', action="store_true")
	parser.add_argument('-c', '--config', help='Specify configuration file', default='etc/scanner.ini', type=argparse.FileType('r'))
	parser.add_argument('-l', '--logfile', help='Specify log file', default='log/scanner.log', type=argparse.FileType('r'))
	parser.add_argument('-t', '--threads', help='Specify worker threads (1 - 32)', metavar='N', type=int, default=1, choices=range(1, 32))
	parser.add_argument('-d', '--directory', help='Specify directory to scan', default='.')
	parser.add_argument('--version', help='Product version', action="store_true")
	args = parser.parse_args()
	if args.verbose:
		print('verbosity turned on')
	if args.config:
		time.sleep(0)
	if args.version:
		print(programname, ' ', programversion)
		return 0
	return(args)

def read_config_file(file):
	import configparser
	config = configparser.ConfigParser()
	config
	config.read(file)
	config.sections()
	['Database', 'Parallelism']
	print("Database type = " + config['Database']['type'])
	print("Database host = " + config['Database']['host'])
	print("Database port = " + config['Database']['port'])
	print("Parallelism threads = " + config['Parallelism']['threads'])
	return 0

def get_tree_size(path,engine,conn,ins,dirs):
	import time
	#from sqlalchemy import create_engine
	#engine = create_engine('sqlite:///:memory:', echo=True)
	#engine = create_engine('sqlite:///scanner.db', echo=False)

	#from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, Numeric, DateTime
	#from sqlalchemy import Sequence
	#Column('id', Integer, Sequence('user_id_seq'), primary_key=True)
	#metadata = MetaData()

	#os.stat_result(st_mode=16832, st_ino=17718552, st_dev=64768, st_nlink=2, st_uid=1000, st_gid=1000, st_size=6, st_atime=1475671485, st_mtime=1475671470, st_ctime=1475671470)

	#directories = Table('directories', metadata, Column('path', String(65535)), Column('mode', Numeric(4)), Column('inode', BigInteger), Column('links', BigInteger), Column('uid', BigInteger), Column('gid', BigInteger), Column('size', BigInteger), Column('atime', DateTime), Column('mtime', DateTime), Column('ctime', DateTime))

	#metadata.create_all(engine)
	#conn = engine.connect()
	#ins = directories.insert()

	import os
	total = 0
	for entry in os.scandir(path):
		try:
			is_dir = entry.is_dir(follow_symlinks=False)
		except OSError as error:
			print('Error calling is_dir():', error, file=sys.stderr)
			continue
		if is_dir:
			dirs += 1
			total += get_tree_size(entry.path,engine,conn,ins,dirs)
			stat =  os.stat(entry.path)
			conn.execute(ins, path=entry.path, mode=stat.st_mode, inode=stat.st_ino, links=stat.st_nlink, uid=stat.st_uid, gid=stat.st_gid, size=stat.st_size, atime=datetime.datetime.fromtimestamp(stat.st_atime), mtime=datetime.datetime.fromtimestamp(stat.st_mtime), ctime=datetime.datetime.fromtimestamp(stat.st_ctime))
		else:
			try:
				total += entry.stat(follow_symlinks=False).st_size
			except OSError as error:
				print('Error calling stat():', error, file=sys.stderr)
	return total

###################################################################################

if __name__ == '__main__':
	args = cli_arguments()
	read_config_file(args.config.name)


	from sqlalchemy import create_engine
	engine = create_engine('sqlite:///:memory:', echo=True)
	#engine = create_engine('sqlite:///scanner.db', echo=False)

	from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, BigInteger, Numeric, DateTime
	from sqlalchemy import Sequence
	Column('id', Integer, Sequence('user_id_seq'), primary_key=True)
	metadata = MetaData()

	# os.stat_result(st_mode=16832, st_ino=17718552, st_dev=64768, st_nlink=2, st_uid=1000, st_gid=1000, st_size=6, st_atime=1475671485, st_mtime=1475671470, st_ctime=1475671470)

	directories = Table('directories', metadata, Column('path', String(65535)), Column('mode', Integer), Column('inode', BigInteger), Column('links', BigInteger), Column('uid', BigInteger), Column('gid', BigInteger), Column('size', BigInteger), Column('atime', DateTime), Column('mtime', DateTime), Column('ctime', DateTime))

	metadata.create_all(engine)
	conn = engine.connect()
	ins = directories.insert()

	dirs = 0

	print('Total size : ', get_tree_size(args.directory,engine,conn,ins,dirs))

	print(socket.getfqdn())



# create logger
logger = logging.getLogger('simple_example')
logger.setLevel(logging.DEBUG)

ch = logging.FileHandler('log/scanner.log')
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

# 'application' code
logger.debug('debug message')
logger.info('info message')
logger.warn('warn message')
logger.error('error message')
logger.critical('critical message')
# Shut down the logger
logging.shutdown()
