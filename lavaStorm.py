#!/usr/bin/env python
# Copyright 2011-2014 David Irvine
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

from olwclient import OpenLavaConnection, Job
from random import randint, choice
import time
import sys
import datetime
import logging
import argparse

"""
.. program:: smartStormRemote

.. option:: --failure_rate

The percent of submitted jobs that will fail of their own accord at a random interval.  0: No jobs will fail, 100: All
jobs will fail.  The default value is 1, that is 1 job in 100 on average will exit with a failure.

.. option:: --office_hours

A list of start and end times in the format HH:MM:SS-HH:MM:SS.  Multiple start and end times are supported. For example,
for a user who is active between 9am and 5pm.::

    --office_hours 09:00:00-17:00:00

For a user who is active between 9am, and 5pm, but takes a 1hr lunch break at midday.::

    --office_hours 09:00:00-12:00:00,13:00:00-17:00:00

The default is to be active 24 hours a day.

.. option:: --min_runtime

The minimum amount of time in seconds a completed job should run for, in seconds. Default: 10 Minutes.

.. option:: --max_runtime

The maximum amount of time in seconds a completed job should run for, in seconds. Default: 10 Minutes.

.. option:: --min_observation_time

The minimum amount of time in seconds a job should be 'observed' before a new job can be submitted.  Observation time is
defined as the amount of time the user spends checking their results before submitting a new job.  The default is 2
minutes.

.. option:: --max_observation_time

The maximum amount of time in seconds a job should be 'observed' before a new job can be submitted.  Observation time is
defined as the amount of time the user spends checking their results before submitting a new job.  The default is 2
minutes.

.. option:: --min_num_processors

The minimum number of processors each job should use.  Default 1

.. option:: --max_num_processors

The maximum number of processors each job should use.  Default 1

.. option:: --delay_time

How long to wait before checking up on things. Default: 60 seconds.

.. option:: --project

The project to submit jobs, if specified multiple times, then a random project from the list will be chosen for e
ach job.

If no projects are specified, then the default project will be used, if it exists.

.. option:: --queue

The queue to submit jobs, if specified multiple times, then a random queue from the list will be chosen for each job.

If no queues are specified, then the default queue will be used.


Profile Specific Options
------------------------

Base Load Profile
^^^^^^^^^^^^^^^^^

The baseload profile maintains a steady run of jobs for a specific user.  If the baseload is 5, then a total of 5
active jobs (Active jobs are defined as jobs that are either Pending or Running.  As jobs exit, new jobs are submitted
after a grace period of observation_time seconds.

Use this profile when you want to maintain a current load on the cluster over a period of time.

.. option:: --base_load

The number of concurrent jobs that should be active at any given time.

"""


class Profile(object):
    connection = None  # OpenlavaWeb Connection

    def __init__(self):
        self.submit_queue = []
        self.pending_jobs = []
        self.running_jobs = []
        self.killed_jobs = []
        self.failed_jobs = []
        self.completed_jobs = []
        self.projects = []
        self.queues = []

        self.failure_rate = 1  # percent
        self.office_hours = [
            {
                'start_time': datetime.time(0, 0, 0),
                'end_time': datetime.time(23, 59, 59),
            },
        ]
        self.min_runtime = 60
        self.max_runtime = 120
        self.min_observation_time = datetime.timedelta(seconds=0)
        self.max_observation_time = datetime.timedelta(seconds=0)
        self.min_num_processors = 1
        self.max_num_processors = 1

    def is_active(self):
        """
        Returns true if the current time is inside the range of times this profile is active.  If there are no active
        'office hours' then always returns true.

        :return: True if the current profile is 'in the office'

        """
        if len(self.office_hours) < 1:
            return True

        n = datetime.datetime.now()
        for h in self.office_hours:
            if h['start_time'] <= n <= h['end_time']:
                return True
        return False

    def get_queue_name(self):
        """
        Gets a random queue to submit jobs to, if no queue exists to chose from, returns None.

        :return: Queue name to submit job to, or None if no queues defined.

        """
        if len(self.projects) > 0:
            return choice(self.projects)
        return None

    def get_project_name(self):
        """
        Gets a random project to submit jobs to, if no project exists to chose from, returns None.

        :return: Project name to submit job to, or None if no projects defined.

        """
        if len(self.projects) > 0:
            return choice(self.projects)
        return None

    def start_job(self, **kwargs):
        """
        Starts a new job, right now, using the job scheduler.

        :param kwargs:  Dictionary containing arguments to be passed to the submit function.

        :return: None

        """
        logging.debug("Starting Job: %s" % kwargs)
        for f in ['project', 'queue']:
            if f in kwargs and not kwargs[f]:
                del (kwargs[f])
        j = Job.submit(self.connection, **kwargs)
        self.pending_jobs.append({'job_id': j.job_id, 'array_index': j.array_index})

    def get_job(self, job_id, array_index):
        """
        Gets a job object for the job.

        :param job_id: job_id of job to get
        :param array_index: array_index of job to get
        :return: Job object

        """
        return Job(self.connection, job_id, array_index)

    def get_active_jobs(self):
        """
        Returns an array of dictionaries containing the job_id and array_index of active jobs.  Active jobs are those
        that are in pending or running state.

        :return: Array of job info dictionaries

        """
        return self.pending_jobs + self.running_jobs

    def kill_all_jobs(self):
        """
        Finds all jobs that are active, and tells the scheduler to kill them.

        :param job_id: Job ID of the job
        :param array_index: Array index of the job
        :return: None

        """
        for jinf in self.get_active_jobs():
            job = self.get_job(jinf['job_id'], jinf['array_index'])
            if job.is_running or job.is_pending:
                logging.debug("Job: %s:%s is active, killing..." % (job.job_id, job.array_index))
                job.kill()

    def get_active_job_count(self):
        """
        Returns the number of jobs that are active right now, that is the totals of the following:

        * jobs that have not yet been submitted
        * jobs that have been submitted and are pending
        * jobs that are currently executing.

        :return: int active_job_count

        """
        return len(self.pending_jobs) + len(self.running_jobs) + len(self.submit_queue)

    def get_num_processors(self):
        """
        Returns the number of processors that should be used, uses a random number between min and max number of
        permitted processors.

        :return: num_processors - integer

        """
        return randint(self.min_num_processors, self.max_num_processors)

    def get_runtime_seconds(self):
        """
        Gets the number of seconds a job should run for.  Used when creating a job to find the runtime that the job
        should run for.

        :return:  runtime_seconds  - the job runtime in seconds as an integer.

        """
        return randint(self.min_runtime, self.max_runtime)

    def get_observation_time(self):
        """
        Returns the number of seconds used to 'Observe' the results of the previous jobs.  This simulates the user
        reviewing their work, prior to submitting a new job.

        :return: observation_time in seconds

        """
        return randint(self.min_observation_time.seconds, self.max_observation_time.seconds)

    def get_next_start_time(self):
        """
        Returns a datetime object when the job should next start.

        :return: datetime object when job should start

        """
        return datetime.datetime.now() + datetime.timedelta(seconds=self.get_observation_time())

    def create_job_command(self):
        """
        Creates the command to run

        :return: cmd that will be executed as part of the job.

        """
        run_time = self.get_runtime_seconds()
        exit_status = 0
        if self.failure_rate >= randint(1, 100):
            run_time = randint(0, run_time)
            exit_status = 1
        cmd = "sleep %s; exit %s" % (run_time, exit_status)
        return cmd

    def process_running_jobs(self):
        """
        Scans all job ids associated with this profile, and moves them into the associated list for
        pending, running, complete, killed, and failed jobs.

        :return: None

        """
        logging.debug("Processing Jobs....")
        jobs_to_check = self.get_active_jobs()
        self.pending_jobs = []
        self.running_jobs = []
        for j in jobs_to_check:
            logging.debug("Checking Job: %s" % j)
            job_id = j['job_id']
            array_index = j['array_index']
            job = self.get_job(job_id, array_index)
            if job.is_running:
                logging.debug("Job %d is Running" % job.job_id)
                self.running_jobs.append(j)
            elif job.is_pending:
                logging.debug("Job %d is Pending" % job.job_id)
                self.pending_jobs.append(j)
            elif job.is_completed:
                logging.debug("Job %d is Completed" % job.job_id)
                self.completed_jobs.append(j)
            elif job.is_failed:
                logging.debug("Job %d is Failed" % job.job_id)
                self.failed_jobs.append(j)
            elif job.was_killed:
                logging.debug("Job %d was Killed" % job.job_id)
                self.killed_jobs.append(j)
            else:
                logging.debug("Job %d is broken... %s" % (job.job_id, job))
                raise ValueError("This shouldn't happen")
        logging.info(
            "Current activity: %d waiting to submit, %d pending, %d running, %d complete, %d failed, %d killed" % (
                len(self.submit_queue),
                len(self.pending_jobs),
                len(self.running_jobs),
                len(self.completed_jobs),
                len(self.failed_jobs),
                len(self.killed_jobs)
            ))

    def run(self):
        """
        Called by the main loop periodically, manage jobs should create, start, stop, and kill processes as required.

        :return: None

        """
        try:
            while True:
                self.process_running_jobs()
                self.create_jobs()
                self.start_jobs()
                time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Terminating. Killing all active jobs.")
            self.kill_all_jobs()

    def start_jobs(self):
        """
        Iterates through the submit_queue, checks it the current time is on or after the earliest time to submit the
        job.  If that is the case, then the job is submitted using start_job().  If it isnt, it is left in the queue.

        :return: None

        """
        jobs = []
        jobs.extend(self.submit_queue)
        logging.debug("Jobs to process: %s" % jobs)
        self.submit_queue = []
        for job in jobs:
            if job['start_time'] <= datetime.datetime.now():
                # Ready to be started
                self.start_job(**job['job'])
            else:
                # Not ready, put back in the queue.
                self.submit_queue.append(job)


class BaseLoadProfile(Profile, object):
    @classmethod
    def add_arguments(cls, sparser):
        sparser.add_argument("--base_load", type=int, default=5,
                             help="The number of concurrent jobs that should be active at any given time.")

    sub_command_name = "baseload"
    sub_command_help = "Maintains a steady number of jobs"

    def __init__(self):
        self.base_load = 0
        super(BaseLoadProfile, self).__init__()

    def create_jobs(self):
        start_time = self.get_next_start_time()
        logging.debug("Active job count: %s" % self.get_active_job_count())
        if self.get_active_job_count() >= self.base_load:
            return

        for i in range(self.get_active_job_count(), self.base_load):
            logging.debug("Adding job to submit queue, start time: %s " % start_time)
            cmd = self.create_job_command()
            job_data = {
                'start_time': start_time,
                'job': {
                    'command': cmd,
                    'num_processors': self.get_num_processors(),
                    'project': self.get_project_name(),
                    'queue': self.get_queue_name(),
                },
            }

            self.submit_queue.append(job_data)


FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

parser = argparse.ArgumentParser(description='Submits load to a batch scheduler')
parser.add_argument("--failure_rate", type=int, choices=range(0, 101), default=1,
                    help="The percent of submitted jobs that will fail of their own accord at a random interval.")
parser.add_argument("--office_hours", type=str, default="00:00:00-23:59:59",
                    help="A list of start and end times in the format HH:MM-HH:MM. ")
parser.add_argument("--min_runtime", type=int, default=600,
                    help="The minimum amount of time a completed job should run for, in seconds. Default: 10 Minutes.")
parser.add_argument("--max_runtime", type=int, default=600,
                    help="The maximum amount of time a completed job should run for, in seconds. Default: 10 Minutes.")
parser.add_argument("--min_observation_time", type=int, default=120,
                    help="The minimum amount of time a job should be 'observed' before a new job can be submitted.")
parser.add_argument("--max_observation_time", type=int, default=120,
                    help="The maximum amount of time a job should be 'observed' before a new job can be submitted.")
parser.add_argument("--min_num_processors", type=int, default=1,
                    help="The minimum number of processors each job should use.  Default 1")
parser.add_argument("--max_num_processors", type=int, default=1,
                    help="The maximum number of processors each job should use.  Default 1")
parser.add_argument("--queue", type=str, dest="queues", action="append", default=[],
                    help="Queue to submit to, if multiple queues are specified, selects one at random.")
parser.add_argument("--project", dest="projects", type=str, action="append", default=[],
                    help="Project to submit to, if multiple queues are specified, selects one at random.")

subparsers = parser.add_subparsers(help='sub-command help')
for cls in Profile.__subclasses__():
    p = subparsers.add_parser(cls.sub_command_name, help=cls.sub_command_help)
    cls.add_arguments(p)
    p.set_defaults(cls=cls)

OpenLavaConnection.configure_argument_list(parser)
args = parser.parse_args()
try:
    if args.office_hours:
        ranges = []
        for r in args.office_hours.split(','):
            start, c, end = r.partition("-")
            start = [int(x) for x in start.split(":")]
            end = [int(x) for x in end.split(":")]
            ranges.append({
                'start_time': datetime.time(*start),
                'end_time': datetime.time(*end),
            })
        args.office_hours = ranges
except ValueError:
    sys.stderr.write("Invalid time range supplied\n")
    sys.exit(1)
args.min_observation_time = datetime.timedelta(seconds=args.min_observation_time)
args.max_observation_time = datetime.timedelta(seconds=args.max_observation_time)

connection = OpenLavaConnection(args)
connection.login()
prof = args.cls()
prof.connection = connection
for k, v in vars(args).iteritems():
    setattr(prof, k, v)

prof.run()