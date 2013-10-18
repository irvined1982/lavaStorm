#!/usr/bin/env python
# Copyright 2011 David Irvine
#
# This file is part of LavaStorm
#
# LavaStorm is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# LavaStorm is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with LavaStorm. If not, see <http://www.gnu.org/licenses/>.
#

import sys
import subprocess
import time
import argparse
import tempfile
from mpi4py import MPI
from itertools import combinations
from array import array
from random import Random


parser = argparse.ArgumentParser(description='Consume arbitrary system resources.')
parser.add_argument("runtime", type=int, help="Run for how many seconds")
parser.add_argument("-n", "--network", action="store_true", default=False, help="Send data between nodes using MPI")
parser.add_argument("-N", "--network_iterations", type=int, default=100, help="Number if iterations of the network test")
parser.add_argument("-c", "--compute", action="store_true", default=False, help="Compute random numbers")
parser.add_argument("-m", "--memory", type=int, default=100, help="Megabytes of RAM to consume")
parser.add_argument("-f", "--file", action="store_true", default=False, help="Write data to files")
parser.add_argument("-F", "--file_size", type=int, default=100, help="File size in gb")
parser.add_argument("-d", "--directory", type=str, default="", help="Location to write files to")
parser.add_argument("-S", "--seek_timeout", type=int, default=120, help="Perform seek operations for this long")

args=parser.parse_args()

end_time=args.runtime+time.time()
memory=args.memory*1024*1024


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
if rank == 0:
	print "Starting run. Current time is: %s ending on or after: %s" % (time.time(), end_time)

# use up some ram
data=array('d')
rand=Random()
while (len(data)*data.itemsize)<=memory:
	data.append(rand.random())

def run_compute_tests(data):
	for i in data:
		z = i/rand.random()

def run_network_tests():
	msg=[]
	# Build a message, of size m
	size=1
	while size <= 32768:
		# Increase the size of msg until it is at least size big.
		while(sys.getsizeof(msg)<size*1024):
			msg.append(rand.random())
		size = size * 2
		# Take turns broadcasting from one process to all other processes.
		comm.Barrier()
		for s in range(comm.Get_size()):
			# Repeat it the specified number of times.
			for attempt in range(args.network_iterations):
				comm.Barrier()
				t=time.time()
				data = comm.bcast(msg, root=s)
				if rank==0:
					print  "Broadcast message size: %s from rank: %s in %s" % (sys.getsizeof(msg), s, time.time()-t)
	
		# Send a message to and from every node
		for (s,r) in combinations(range(comm.Get_size()),2):
			for attempt in range(args.network_iterations):
				comm.Barrier()
				t=time.time()
				if rank==s:
					comm.send(msg, dest=1, tag=11)
				elif rank==r:
					data=comm.recv(source=s,tag=11)
					print "Got message size: %s from rank: %s to: %s in %s" % (sys.getsizeof(data), s,r, time.time()-t)

from random import randint

def run_file_tests():
	if (args.directory)>0:
		f=tempfile.TemporaryFile(dir=args.directory )
	else:
		f=tempfile.TemporaryFile()
	max_size=1024*1024*args.file_size # number of bytes to write
	t=time.time()
	print "Writing %s mb to directory: %s one char at a time." % ( args.file_size, args.directory )
	for i in xrange(max_size):
		f.write(str(randint(0,9)))
		f.flush()
	print "Wrote %s mb to directory: %s in %s seconds." % ( args.file_size, args.directory, time.time()-t)
	print "Randomly Seeking around the file"
	t=time.time()
	seeks=0
	while(time.time()<t+args.seek_timeout):
		f.seek(randint(0,max_size))
		f.read(1)
		f.write(str(randint(0,9)))
		f.flush()
		seeks+=1
	print "Seeked, Read and Wrote for %s seconds, total: %s iterations." % (args.seek_timeout, seeks)
	f.seek(0)
	f.truncate()
	t=time.time()
	print "Writing %s mb to directory: %s using buffered io." % ( args.file_size, args.directory )
	for i in xrange(max_size/1024*1024):
		f.write("".join([str(randint(0,9)) for i in xrange(1024*1024)]))
	f.flush()
	print "Wrote %s mb to directory: %s in %s seconds." % ( args.file_size, args.directory, time.time()-t)
	


while(time.time()<=end_time):
	if args.network:
		run_network_tests()
	if args.compute:
		run_compute_tests(data)
	if args.file:
		run_file_tests()

if rank == 0:
	print "Finishing run. Current time is: %s ending on or after: %s" % (time.time(), end_time)
