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
import time
import argparse
import tempfile
from array import array
from random import Random
import os
import logging

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)


class Test:
    def test(self, cycle_time):
        logging.debug("Starting test cycle for module: %s for %s seconds" % (self.name, cycle_time))
        run_until = time.time() + cycle_time
        while time.time() <= run_until:
            self.test_cycle()


class CPUIntegerTest(Test):
    name = "Cpu Integer Module"

    def __init__(self, memory):
        logging.info("Initializing Integer Module: creating array")
        self._memory = memory
        self._data = array('L')
        self._rand = Random()
        while (len(self._data) * self._data.itemsize) <= self._memory:
            self._data.append(self._rand.randint(0, 0xFFFFFFFFFFFFFFFF))
        self._num_elements = len(self._data)
        self._cur_element = 0
        logging.info("Initializing Integer Module: Created array")

    def test_cycle(self):
        for i in xrange(1000):
            try:
                self._data[self._cur_element] += self._rand.randint(0, 0xFFFFFFFFFFFFFFFF)
            except OverflowError:
                self._data[self._cur_element] = self._rand.randint(0, 0xFFFFFFFFFFFFFFFF)
        self._cur_element += 1
        if self._cur_element >= self._num_elements:
            self._cur_element = 0


class CPUFloatTest(Test):
    name = "Cpu Floating Point Module"

    def __init__(self, memory):
        logging.info("Initializing Floating Point Module: Creating array")
        self._memory = memory
        self._data = array('d')
        self._rand = Random()
        while (len(self._data) * self._data.itemsize) <= self._memory:
            self._data.append(self._rand.random())
        self._num_elements = len(self._data)
        self._cur_element = 0
        logging.info("Initializing Floating Point Module: Created array")

    def test_cycle(self):
        for i in xrange(1000):
            self._data[self._cur_element] /= self._rand.random()
        self._cur_element += 1
        if self._cur_element >= self._num_elements:
            self._cur_element = 0


class DiskTest(Test):
    name = "Data IO Module"

    def __init__(self, size, directory, sw_ratio, sr_ratio, rr_ratio, rw_ratio):
        logging.info("Initializing Disk Module: Creating File")
        self._file = tempfile.TemporaryFile(dir=directory, suffix="bench.%s" % os.getpid())
        self.file_size = size * 1024 * 1024
        self._file.write("\0" * self.file_size)
        logging.info("Initializing Disk Module: Created File")
        self.sw_ratio = sw_ratio
        self.sr_ratio = sr_ratio
        self.rr_ratio = rr_ratio
        self.rw_ratio = rw_ratio

    def test_cycle(self):
        for i in range(self.sw_ratio):
            self._serial_write()
        for i in range(self.sr_ratio):
            self._serial_read()
        for i in range(self.rr_ratio):
            self._random_read()
        for i in range(self.rw_ratio):
            self._random_write()

    def _random_write(self):
        logging.debug("Starting Random Write.")
        r = Random()
        self._file.seek(r.randint(0, self.file_size))
        self._file.write("\0" * 1024)
        logging.debug("Finished Random Write.")
    def _random_read(self):
        logging.debug("Starting Random Read.")
        r = Random()
        self._file.seek(r.randint(0, self.file_size))
        data = self._file.read(1024)
        logging.debug("Finished Random Read.")

    def _serial_read(self):
        logging.debug("Starting Serial Read.")
        self._file.seek(0)
        total = 200 * 1024 * 1024
        while ( total > 0):
            data = self._file.read(total)
            total -= len(data)
        logging.debug("Finished Serial Read.")

    def _serial_write(self):
        logging.debug("Starting Serial Write.")
        # Write ~200Mb of data
        self._file.seek(0)
        r = Random()
        written = 0
        location = 0
        while written < 200:
            print "loop: %s"  % written
            self._file.write("X" * 1024*1024*10)
            written += 10
            location += 1024 * 1024 * 10
            if location > self.file_size:  # don't get too big
                self._file.seek(0)
        logging.debug("Finished Serial Write.")

class NetworkTest(Test):
    name = "Network IO Module"
    def __init__(self):
        from mpi4py import MPI
        logging.info("Initializing Network IO Module")
        self._comm = MPI.COMM_WORLD
        fmt = "Rank: %s:" % self._comm.rank
        fmt += '%(asctime)s:%(levelname)s:%(message)s'
        logging.basicConfig(format=fmt)
        logging.debug("Network IO: Blocking on mpi init")
        self._comm.Barrier()
        logging.debug("Network IO: Synced on mpi init")

    def test_cycle(self):
        r=Random()
        logging.debug("Network IO: Blocking on test_cycle init")
        self._comm.Barrier()
        logging.debug("Network IO: Synced on test_cycle init.  Sending messages")
        sizes=[1024, 2048, 4096]  #k
        for size in sizes:
            msg=[]
            while(sys.getsizeof(msg)<size*1024):
                msg.append(r.random())
            self._comm.Barrier()
            for s in range(self._comm.Get_size()):
                for tr in xrange(10):
                    self._comm.Barrier()
                    t=time.time()
                    data = self._comm.bcast(msg, root=s)
                    if self._comm.rank==0:
                        logging.info("Network IO: Broadcast message size: %s from rank: %s in %s" % (sys.getsizeof(msg), s, time.time()-t))




def get_ratio(a, b):
    if a == 0:
        return 0
    if a == b:
        return 1
    if b == 0:
        return 1
    return (float(float(a) / float(b)))

    if b == 0:
        return 1
    if a > b:
        return 1 - (float(float(a) / float(b)))
    else:
        return (float(float(a) / float(b)))


parser = argparse.ArgumentParser(description='Consume arbitrary system resources.')

parser.add_argument("runtime", type=int, help="Run for how many seconds")
parser.add_argument("-n", "--enable_network", action="store_true", default=False,
                    help="Send data between processes using MPI")
parser.add_argument("-N", "--network_ratio", type=int, default=1,
                    help="What ratio of time to spend on network throughput")

parser.add_argument("-c", "--enable_cpu", action="store_true", default=False, help="Consume CPU Time")
parser.add_argument("-C", "--cpu_ratio", type=int, default=1, help="What ratio of time to spend burning CPU time")
parser.add_argument("--cpu_fpoint_ratio", type=int, default=1,
                    help="Ratio of CPU time to spend on floating point calculations")
parser.add_argument("--cpu_integer_ratio", type=int, default=1,
                    help="Ratio of CPU time to spend on integer calculations")
parser.add_argument("-m", "--memory", type=int, default=2048, help="Megabytes of RAM to consume")

parser.add_argument("-d", "--enable_data", action="store_true", default=False,
                    help="Perform data read/write operations")
parser.add_argument("-D", "--data_ratio", type=int, default=1, help="What ratio of time to spend on data operations")

system_tmp_dir = "/tmp"  #FIX THIS TO USE ENV first....
system_ram_size = 4096

parser.add_argument("--data_dir", default=system_tmp_dir, help="Directory to use for writing data")
parser.add_argument("--data_size", type=int, default=system_ram_size * 2, help="Size of data files in MB")
parser.add_argument("--data_stream_write_ratio", type=int, default=1,
                    help="Ratio of disk time to spend writing stream data")
parser.add_argument("--data_stream_read_ratio", type=int, default=1,
                    help="Ratio of disk time to spend reading stream data")
parser.add_argument("--data_random_write_ratio", type=int, default=1,
                    help="Ratio of disk time to spend writing random char data")
parser.add_argument("--data_random_read_ratio", type=int, default=1,
                    help="Ratio of disk time to spend reading random char data")

parser.add_argument("--cycle_time", type=int, default=60, help="Amount of time to spend on cycle")

args = parser.parse_args()

tests = []

if args.enable_cpu:
    logging.info("Configuring FP math module")
    tests.append(
        (

            CPUFloatTest(args.memory),
            get_ratio(args.cpu_fpoint_ratio, (args.cpu_integer_ratio + args.cpu_fpoint_ratio)) * args.cpu_ratio,
        )
    )
    logging.info("Configuring Integer math module")
    tests.append(
        (
            CPUIntegerTest(args.memory),
            get_ratio(args.cpu_integer_ratio, (args.cpu_integer_ratio + args.cpu_fpoint_ratio)) * args.cpu_ratio,
        )
    )

if args.enable_data:
    logging.info("Configuring Disk Module")
    tests.append(
        (
            DiskTest(args.data_size, directory=args.data_dir, sw_ratio=args.data_stream_write_ratio,
                     sr_ratio=args.data_stream_read_ratio, rr_ratio=args.data_random_read_ratio,
                     rw_ratio=args.data_random_write_ratio),
            args.data_ratio
        )
    )
if args.enable_network:
    logging.info("Configuring Network Module")
    tests.append(
        (
            NetworkTest(),
            args.network_ratio
        )
    )

if len(tests) < 1:
    parser.error("No modules enabled.")

tots = 0
for t in tests:
    tots += t[1]
avg = float(args.cycle_time) / float(len(tests))

logging.info("Initiation complete, starting execution")
end_time = args.runtime + time.time()

try:
    while time.time() < end_time:
        logging.debug("Starting new test cycle")
        for t in tests:
            rt = float(get_ratio(t[1], tots)) * args.cycle_time
            t[0].test(rt)
    logging.info("Execution complete, shutting down")

except KeyboardInterrupt:
    print "Aborted by user.  Shutting down...."

