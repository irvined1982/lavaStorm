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
args=parser.parse_args()

r=Random()

# For each job...
for i in xrange(args.num_jobs):
	# Define base bsub command in command_str
	command_str=args.bsub_command
	# Split command_str into list
	command=command_str.split()
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

	command.append( 'mpirun -n %d ./consumeResources.py %d --memory %d -n -c' % (num_cores, job_runtime, mem_size ) )
	subprocess.call(command)

