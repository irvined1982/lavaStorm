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

import argparse
import sys
import subprocess

from random import Random
parser = argparse.ArgumentParser(description='Flood OpenLava with some jobs')
parser.add_argument('num_jobs', type=int)
parser.add_argument('--bsub_command', type=str,default='bsub')
parser.add_argument('--min_runtime', type=int, default=80)
parser.add_argument('--max_runtime', type=int, default=160)
parser.add_argument('--queue',action='append')
parser.add_argument('--project',action='append')
parser.add_argument('--min_mem', type=int, default=1)
parser.add_argument('--max_mem', type=int, default=1)
parser.add_argument('--min_cores', type=int, default=1)
parser.add_argument('--max_cores', type=int, default=1)
parser.add_argument("-n", "--network", action="store_true", default=False, help="Send data between nodes using MPI")
parser.add_argument("-N", "--network_iterations", type=int, default=100, help="Number if iterations of the network test")
parser.add_argument("-c", "--compute", action="store_true", default=True, help="Compute random numbers")
parser.add_argument("-m", "--memory", type=int, default=100, help="Megabytes of RAM to consume")
parser.add_argument("-f", "--file", action="store_true", default=False, help="Write data to files")
parser.add_argument("-F", "--file_size", type=int, default=100, help="File size in gb")
parser.add_argument("-d", "--directory", type=str, default="", help="Location to write files to")
parser.add_argument("-S", "--seek_timeout", type=int, default=120, help="Perform seek operations for this long")

args=parser.parse_args()

r=Random()

# For each job...
for i in xrange(args.num_jobs):
	#TODO: This doesn't deal with options that have spaces, such as -R "foo bar"
	command=args.bsub_command.split()
	# Pick a random number of cores to use
	num_cores=r.randint( args.min_cores, args.max_cores )
	command.append( '-n' )
	command.append( str(num_cores) )

	# Pick a random runtime between the min and max length
	job_runtime=r.randint( args.min_runtime, args.max_runtime)
	# Pick a random amount of memory to consume
	mem_size=r.randint( args.min_mem, args.max_mem )

	# Pick a queue if needed.
	if args.queue:
		command.append( '-q' )
		command.append( r.choice( args.queue ) )

	# Pick a project if needed
	if args.project:
		command.append( '-P' )
		command.append( r.choice( args.project ) )

	enabled_features=""
	if args.network:
		enabled_features+="-n "
	if args.file:
		enabled_features += "-f "
	if args.compute:
		enabled_features += "-c "

	command.append( 'mpirun -n %d ./consumeResources.py %d --memory %d %s --file_size="%s" --directory="%s" --seek_timeout="%s"' % (num_cores, job_runtime, mem_size, enabled_features, args.file_size, args.directory, args.seek_timeout) )
	subprocess.call(command)

