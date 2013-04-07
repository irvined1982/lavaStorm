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
from array import array
from random import Random
import time


memory=int(sys.argv[1])
memory=memory*1024*1024 # bytes
end_time=int(sys.argv[2])
end_time=end_time+time.time()

data=array('d')
rand=Random()
while (len(data)*data.itemsize)<=memory:
	data.append(rand.random())

while(time.time()<=end_time):
	for i in data:
		z= i/rand.random()

