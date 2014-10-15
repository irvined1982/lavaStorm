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

.. option:: --min_tasks_per_job

The minimum number of tasks per job.  The actual number of tasks in the job will be a random number between the min and
maximum values inclusive.  Default: 1.

.. option:: --max_tasks_per_job

The maximum number of tasks per job.  The actual number of tasks in the job will be a random number between the min and
maximum values inclusive.  Default  1.

.. option:: --scheduler

The Scheduler interface to use, can be one of: sge_cli,openlava_cli,openlava_cluster_api,openlava_web,openlava_c_api

Scheduler Specific Options
--------------------------

Openlava Command Line Interface
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. option:: --bsub_command

The path to the bsub command, additional arguments can also be passed.

Openlava Web
^^^^^^^^^^^^

.. option:: --url

The URL to submit jobs to if using openlava web.

.. option:: --username

The username of the account used when submitting jobs through openlava web.

.. option:: --password

The password of the account used when submitting jobs through openlava web.

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

Submit Batch Profile
^^^^^^^^^^^^^^^^^^^^

The SubmitBatch profile submits a large number of jobs all at once, then waits for them to complete.  After a some
observation time period, another batch is submitted, and the process repeats.

No new jobs are submitted, until all the previous jobs have exited.

Use this profile when you want to create a peak in demand.

..option:: --min_num_jobs_per_batch

The minimum number of jobs that should be submitted each time

..option:: --max_num_jobs_per_batch

The maximum number of jobs that should be submitted each time

..option:: --iterations

The number of iterations to do before exiting.

"""

from random import randint, choice
import time
import sys
import datetime
import logging
import argparse
import re
import subprocess
from xml.dom import minidom
from olwclient import OpenLavaConnection, Job


class SimpleJob(object):
    """
    SimpleJob is a very simple job implementation providing just enough information for LavaStorm to make decisions, and
    no more.  Job Managers are free to either subclass SimpleJob, or use a different implementation entirely however
    the following methods and attributes MUST be implemented.

    At least one of the following attributes must be true.  All must be defined.

    .. py:attribute:: is_running

        True if the job is executing on the cluster.

    .. py:attribute:: is_pending

        True if the job is pending execution

    .. py:attribute:: is_completed

        True if and only if the job has completed successfully.

    .. py:attribute:: is_failed

        True if and only if the job failed

    .. py:attribute:: is_suspended

        True if the job is currently suspended

    .. py:attribute:: was_killed

        True if the job was killed by the end user or administrator

    .. py:attribute:: job_id

        The numerical Job ID

    .. py:attribute:: array_index

        The numerical array index, zero if not part of an array,

    .. py:method:: kill()

        Kill the job using the job scheduler.

    """

    def __init__(self, job_id, array_index, is_running=False, is_pending=False, is_completed=False, is_failed=False,
                 is_suspended=False, was_killed=False):
        self.is_running = is_running
        self.is_pending = is_pending
        self.is_completed = is_completed
        self.is_failed = is_failed
        self.is_suspended = is_suspended
        self.was_killed = was_killed
        self.job_id = int(job_id)
        self.array_index = int(array_index)

    def kill(self):
        raise NotImplementedError


class JobManager(object):
    """
    Job Managers are responsible for submitting, monitoring the state of, and killing jobs.
    """
    def __init__(self):
        self.args = None

    @classmethod
    def add_argparse_arguments(cls, parser):
        """
        Adds any additional command line arguments needed by the job manager.
        """
        pass

    def initialize(self, parsed_args):
        """
        Called when the job manager is selected by the user.
        """
        self.args = parsed_args

    # def start_job(self, num_tasks, requested_slots=None, project_name=None, command=None, queue_name=None):
    #     """
    #     Submits jobs into the job scheduler, returns a list of submitted jobs as an array of dictionaries,
    #     each element containing the job_id and array_index.
    #     """
    #     raise NotImplementedError

    def get_jobs(self, job_id):
        """
        Gets all jobs for a specified job id. Returns a list of objects that implement SimpleJob
        """
        pass

    def get_job(self, job_id, array_index):
        """
        Gets a single job specified by the job id and array index.  Returns an object that implements SimpleJob
        """
        pass


class Profile(object):
    sub_command_name = "Not Defined"
    sub_command_help = "Not Defined"

    def __init__(self):
        self.manager = None
        self.submit_queue = []
        self.active_jobs = []

        self.total_submitted_jobs = 0
        self.total_active_jobs = 0
        self.total_finished_jobs = 0

        self.total_task_count = 0
        self.pending_task_count = 0
        self.running_task_count = 0
        self.killed_task_count = 0
        self.completed_task_count = 0
        self.failed_task_count = 0

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
        self.min_tasks_per_job = 1
        self.max_tasks_per_job = 1

    @classmethod
    def add_arguments(cls, sub_parser):
        """
        Adds any command line arguments needed by the profile.  Arguments must not be positional arguments, and should
        where possible set default values.  Must not return any value.

        :param sub_parser: Sub parser object that arguments should be added to.
        :return: None

        """
        return None

    def is_active(self):
        """
        Returns true if the current time is inside the range of times this profile is active.  If there are no active
        'office hours' then always returns true.

        :return: True if the current profile is 'in the office'

        """
        if len(self.office_hours) < 1:
            return True

        for h in self.office_hours:
            if h['start_time'] <= datetime.datetime.now().time() <= h['end_time']:
                return True
        return False

    def get_queue_name(self):
        """
        Gets a random queue to submit jobs to, if no queue exists to chose from, returns None.

        :return: Queue name to submit job to, or None if no queues defined.

        """
        if len(self.queues) > 0:
            return choice(self.queues)
        return None

    def get_project_name(self):
        """
        Gets a random project to submit jobs to, if no project exists to chose from, returns None.

        :return: Project name to submit job to, or None if no projects defined.

        """
        if len(self.projects) > 0:
            return choice(self.projects)
        return None

    def start_job(self, num_tasks=1, **kwargs):
        """
        Starts a new job, right now, using the job scheduler.

        :param num_tasks: Number of tasks contained by job
        :param kwargs:  Dictionary containing arguments to be passed to the submit function.

        :return: None

        """

        logging.debug("Starting Job: %s" % kwargs)

        self.active_jobs.extend(self.manager.start_job(num_tasks, **kwargs))
        logging.debug("Current active job list is: %s" % len(self.active_jobs))
        self.pending_task_count += num_tasks
        self.total_task_count += num_tasks
        self.total_submitted_jobs += 1

    def get_job(self, job_id, array_index):
        """
        Gets a job object for the job.

        :param job_id: job_id of job to get
        :param array_index: array_index of job to get
        :return: Job object

        """
        return self.manager.get_job(job_id, array_index)

    def get_jobs(self, job_id):
        """
        Gets all tasks for specified job id

        :param job_id: Id of jot
        :return: list of job objects
        :rtype: list

        """
        return self.manager.get_jobs(job_id)

    def get_num_tasks(self):
        """
        Gets the number of tasks for the job.

        :return: randint between min and max tasks per job.

        """
        return randint(self.min_tasks_per_job, self.max_tasks_per_job)

    def kill_all_jobs(self):
        """
        Finds all jobs that are active, and tells the scheduler to kill them.

        :return: None

        """
        for jinf in self.active_jobs:
            job = self.get_job(jinf['job_id'], jinf['array_index'])
            if job.is_running or job.is_pending:
                logging.debug("Job: %s:%s is active, killing..." % (job.job_id, job.array_index))
                try:
                    # Allow this to fail, job might have finished, etc...
                    job.kill()
                except:
                    pass

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

        :return: observation_time as TimeDelta

        """
        return datetime.timedelta(seconds=randint(self.min_observation_time.seconds, self.max_observation_time.seconds))

    def get_next_start_time(self):
        """
        Returns a datetime object when the job should next start.

        :return: datetime object when job should start

        """
        return datetime.datetime.now() + self.get_observation_time()

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

        # Set of job ids that need to be downloaded and checked
        active_job_ids = set()
        # job ids that were checked and found to be active
        ajids_for_total = set()
        # tasks that are active
        active_jobs = []
        # totals
        self.pending_task_count = 0
        self.pending_task_count = 0
        self.running_task_count = 0
        self.killed_task_count = 0
        self.completed_task_count = 0

        for j in self.active_jobs:
            active_job_ids.add(j['job_id'])

        for jid in active_job_ids:
            for job in self.get_jobs(jid):
                logging.debug("Checking job: %d[%d]." % (job.job_id, job.array_index))
                if job.is_running or job.is_suspended:
                    ajids_for_total.add(job.job_id)
                    logging.debug("Job %d is Running" % job.job_id)
                    self.running_task_count += 1
                    active_jobs.append(j)
                elif job.is_pending:
                    ajids_for_total.add(job.job_id)
                    logging.debug("Job %d is Pending" % job.job_id)
                    active_jobs.append(j)
                    self.pending_task_count += 1
                elif job.is_completed:
                    self.completed_task_count += 1
                    logging.debug("Job %d is Completed" % job.job_id)
                elif job.is_failed:
                    self.failed_task_count += 1
                    logging.debug("Job %d is Failed" % job.job_id)
                elif job.was_killed:
                    self.killed_task_count += 1
                    logging.debug("Job %d was Killed" % job.job_id)
                else:
                    logging.debug("Job %d is broken... %s" % (job.job_id, job))
                    raise ValueError("This shouldn't happen")
        self.total_active_jobs = len(ajids_for_total) + len(self.submit_queue)
        self.total_finished_jobs = self.total_submitted_jobs - self.total_active_jobs

        logging.info("Job Activity: %d jobs total, %d jobs waiting to submit, %d jobs active, %d jobs finished. " %
                     (
                         self.total_submitted_jobs,
                         len(self.submit_queue),
                         self.total_active_jobs,
                         self.total_finished_jobs,
                     ))
        logging.info(
            "Task Activity: %d total, %d pending, %d running, %d killed, %d complete, %d failed." %
            (
                self.total_task_count,
                self.pending_task_count,
                self.running_task_count,
                self.killed_task_count,
                self.completed_task_count,
                self.failed_task_count
            )
        )

    def create_jobs(self):
        raise NotImplementedError

    def run(self):
        """
        Called by the main loop periodically, manage jobs should create, start, stop, and kill processes as required.

        :return: None

        """
        try:
            while True:
                self.process_running_jobs()
                if self.is_active():
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


class DirectSGEManager(JobManager):
    """
    Job Manager for Sun Grid Engine using the Command Line Interface.
    """
    scheduler_name = "sge_cli"

    @classmethod
    def add_argparse_arguments(cls, parser):
        parser.add_argument("--qsub_command", type=str, default="bsub",
                            help="The path to the bsub command, additional arguments can also be passed")
        parser.add_argument("--qsub_pe_type", type=str, default="orte",
                            help="The parallel environment to use when launching parallel jobs")

    def __init__(self):
        super(DirectSGEManager, self).__init__()
        self.job_sizes = {}

    def start_job(self, num_tasks, requested_slots=None, project_name=None, command=None, queue_name=None):
        job_command = self.args.qsub_command.split()

        if requested_slots:
            job_command.append("-pe")
            job_command.append(self.args.qsub_pe_type)
            job_command.append("%s" % requested_slots)

        if num_tasks > 1:
            job_command.append("-t")
            job_command.append("1-%d" % num_tasks)

        if project_name:
            job_command.append("-P")
            job_command.append(project_name)

        if queue_name:
            job_command.append("-q")
            job_command.append(queue_name)

        job_command.append(command)
        logging.debug("Submitting job: %s" % " ".join(job_command))
        output = subprocess.check_output(job_command)
        if num_tasks > 1:
            match = re.search(r'Your job-array (\d+).* has been submitted', output)
        else:
            match = re.search(r'Your job (\d+).* has been submitted', output)

        job_id = match.group(1)

        if num_tasks > 1:
            jobs = [{'job_id': job_id, 'array_index': a_id} for a_id in range(1, num_tasks+1)]
            self.job_sizes[job_id] = num_tasks
        else:
            jobs = [{'job_id': job_id, 'array_index': 0}]
            self.job_sizes[job_id] = 0

        return jobs

    def get_jobs(self, job_id):
        jobs = []
        for i in range(1, self.job_sizes[job_id]+1):
            jobs.append(self.get_job(job_id, i))
        if len(jobs) == 0:
            jobs.append(self.get_job(job_id, 0))
        return jobs

    @staticmethod
    def _get_from_qhist(job_id, array_index):

        # Try to get job from accounting (ie, job is done)
        try:
            is_completed = False
            is_failed = False
            was_killed = False
            cmd = ["qacct", "-j", "%d" % job_id, "-t", "%d" % array_index]
            logging.debug("Looking for jobs with command: %s" % " ".join(cmd))
            output = subprocess.check_output(cmd)
            logging.debug("Output from qstat: %s" % output)
            for line in output.splitlines():
                components = line.split()
                if components[0] == "exit_status":
                    if int(components[1]) == 0:
                        is_completed = True
                    else:
                        is_failed = True
                if components[0] == "failed":
                    if components[1] != 0:
                        is_failed = True
            return SGEDirectJob(job_id, array_index,
                                is_completed=is_completed,
                                is_failed=is_failed,
                                was_killed=was_killed)
        except subprocess.CalledProcessError:
            return None

    @staticmethod
    def _get_from_qstat(job_id, array_index):
        try:
            cmd = ["qstat", "-g", "d", "-xml"]

            logging.debug("Looking for jobs with command: %s" % " ".join(cmd))
            output = subprocess.check_output(cmd)
            logging.debug("Output from qstat: %s" % output)
            xmldoc = minidom.parseString(output)
            jobs = xmldoc.getElementsByTagName('job_list')
            for j in jobs:
                e_job_id = j.getElementsByTagName('JB_job_number')[0].text
                if job_id != e_job_id:
                    continue
                if array_index != 0 and j.getElementsByTagName('tasks').text != array_index:
                    continue

                state = j.getElementsByTagName('state').text
                if state == "qw":
                    return SGEDirectJob(job_id, array_index, is_pending=True)
                if state == "r":
                    return SGEDirectJob(job_id, array_index, is_running=True)

        except subprocess.CalledProcessError:
            return None

    def get_job(self, job_id, array_index):
        if array_index is None:
            array_index = 0

        job = self._get_from_qhist(job_id, array_index)
        if job:
            return job

        job = self._get_from_qstat(job_id, array_index)
        if job:
            return job

        # not in qstat, but not in qhist, try for 10 more seconds to get the job.
        for i in range(10):
            job = self._get_from_qhist(job_id, array_index)
            if job:
                return job
            time.sleep(1)

        return SGEDirectJob(job_id, array_index, was_killed=True, is_failed=True)


class SGEDirectJob(SimpleJob):
    """
    SimpleJob Implementation for Sun Grid Engine
    """

    def kill(self):
        cmd = list("qdel")
        cmd.append("%s" % self.job_id)
        if self.array_index is not 0:
            cmd.append("-t")
            cmd.append("%s" % self.array_index)
        subprocess.check_output(cmd)


class OpenLavaDirectJob(SimpleJob):
    """
    SimpleJob Implementation for Open Lava
    """

    def kill(self):
        cmd = ["bkill"]
        if self.array_index is not 0:
            cmd.append("%s[%s]" % (self.job_id, self.array_index))
        else:
            cmd.append("%s" % self.job_id)
        subprocess.check_output(cmd)


class DirectOpenLavaManager(JobManager):
    scheduler_name = 'openlava_cli'

    @classmethod
    def add_argparse_arguments(cls, parser):
        parser.add_argument("--bsub_command", type=str, default="bsub",
                            help="The path to the bsub command, additional arguments can also be passed")

    def start_job(self, num_tasks, requested_slots=None, project_name=None, command=None, queue_name=None):
        job_command = self.args.bsub_command.split()

        if requested_slots:
            job_command.append("-n")
            job_command.append("%s" % requested_slots)

        if num_tasks > 1:
            job_name = "LavaStorm[1-%d]" % num_tasks
            job_command.append("-J")
            job_command.append(job_name)

        if project_name:
            job_command.append("-P")
            job_command.append(project_name)

        if queue_name:
            job_command.append("-q")
            job_command.append(queue_name)

        job_command.append(command)
        logging.debug("Submitting job: %s" % " ".join(job_command))
        output = subprocess.check_output(job_command)
        match = re.search(r'Job <(\d+)> is submitted to.*', output)
        job_id = match.group(1)
        jobs = [{'job_id': job_id, 'array_index': 0}]

        if num_tasks > 1:
            jobs = []
            bjobs_command = ["bjobs", "-w", "-a", "%s" % job_id]
            output = subprocess.check_output(bjobs_command)
            for line in output.splitlines():
                match = re.search(r'.*LavaStorm\[\d+\]', line)
                array_id = match.group(1)
                jobs.append({'job_id': job_id, 'array_index': array_id})

        return jobs

    def get_jobs(self, job_id):
        bjobs_command = ["bjobs", "-w", "-a", "%s" % job_id]
        logging.debug("Looking for jobs with command: %s" % " ".join(bjobs_command))
        output = subprocess.check_output(bjobs_command)
        logging.debug("Output from bjobs: %s" % output)
        lines = output.splitlines()
        jobs = []
        lines.pop(0)
        logging.debug("Lines: %s" % lines)
        for line in lines:
            if len(lines) > 1:
                # get array index
                match = re.search(r'.*LavaStorm\[\d+\]', line)
                array_index = match.group(1)
            else:
                array_index = 0
            jobs.append(self.get_job(job_id, array_index))
        return jobs

    def get_job(self, job_id, array_index):
        if array_index != 0:
            job = "%s[%s]" % (job_id, array_index)
        else:
            job = "%s" % job_id

        bjobs_command = ["bjobs", "-w", "-a", job]
        logging.debug("Looking for jobs with command: %s" % " ".join(bjobs_command))
        output = subprocess.check_output(bjobs_command)
        logging.debug("Output from bjobs: %s" % output)
        output = output.splitlines()
        output.pop(0)
        entries = output.pop(0).split()
        state = entries[2]
        states = {
            "PEND": {
                "is_running": False,
                "is_pending": True,
                "is_suspended": False,
                "is_completed": False,
                "is_failed": False,
                "was_killed": False,
            },
            "PSUSP": {
                "is_running": False,
                "is_pending": False,
                "is_suspended": True,
                "is_completed": False,
                "is_failed": False,
                "was_killed": False,
            },
            "RUN": {
                "is_running": True,
                "is_pending": False,
                "is_suspended": False,
                "is_completed": False,
                "is_failed": False,
                "was_killed": False,
            },
            "USUSP": {
                "is_running": False,
                "is_pending": False,
                "is_suspended": True,
                "is_completed": False,
                "is_failed": False,
                "was_killed": False,
            },
            "SSUSP": {
                "is_running": False,
                "is_suspended": True,
                "is_pending": False,
                "is_completed": False,
                "is_failed": False,
                "was_killed": False,
            },
            "DONE": {
                "is_running": False,
                "is_pending": False,
                "is_suspended": False,
                "is_completed": True,
                "is_failed": False,
                "was_killed": False,
            },
            "EXIT": {
                "is_running": False,
                "is_pending": False,
                "is_suspended": False,
                "is_completed": False,
                "is_failed": True,
                "was_killed": False,
            },
            "UNKWN": {
                "is_running": True,
                "is_pending": False,
                "is_suspended": False,
                "is_completed": False,
                "is_failed": False,
                "was_killed": False,
            },
            "ZOMBI": {
                "is_running": True,
                "is_pending": False,
                "is_suspended": False,
                "is_completed": False,
                "is_failed": False,
                "was_killed": False,
            },
        }

        return OpenLavaDirectJob(job_id, array_index, **states[state])


class OpenLavaClusterAPIManager(JobManager):
    scheduler_name = "openlava_cluster_api"


class OpenLavaRemoteManager(JobManager):
    """
    Job Manager for the OpenLavaWeb server.  Uses REST calls to manage jobs.

    """
    scheduler_name = "openlava_web"

    @classmethod
    def add_argparse_arguments(cls, parser):
        parser.add_argument("--url", type=str, default=None,
                            help="The URL of the openlava web server.")
        parser.add_argument("--username", type=str, default=None,
                            help="The username of the account used when submitting jobs through openlava web.")
        parser.add_argument("--password", type=str, default=None,
                            help="The password of the account used when submitting jobs through openlava web.")


    def initialize(self, args):
        logging.debug("Initializing Job Manager for OpenLava Web Interface")
        connection = OpenLavaConnection(args)
        logging.debug("Logging in...")
        connection.login()
        logging.debug("Initialized.")
        self.connection = connection

    def start_job(self, num_tasks, **kwargs):
        if num_tasks > 1:
            kwargs['job_name'] = "LavaStorm[1-%d]" % num_tasks

        for f in ['project_name', 'queue_name']:
            if f in kwargs and not kwargs[f]:
                del (kwargs[f])
        return [
            {'job_id': j.job_id, 'array_index': j.array_index} for j in Job.submit(self.connection, **kwargs)
        ]

    def get_jobs(self, job_id):
        return Job.get_job_list(self.connection, job_id, -1)

    def get_job(self, job_id, array_index):
        return Job(self.connection, job_id, array_index)


class OpenLavaCAPIManager(JobManager):
    scheduler_name = "openlava_c_api"

    def start_job(self):
        pass

    def get_jobs(self, job_id):
        pass

    def get_job(self, job_id, array_index):
        pass

    def submit_job(self, **kwargs):
        pass

    class SimpleJob(object):
        def __init__(self, job_id, array_index=None):
            pass

        def kill(self):
            pass


class BaseLoadProfile(Profile, object):
    """
    The baseload profile maintains a steady run of jobs for a specific user.  If the baseload is 5, then a total of 5
    active jobs (Active jobs are defined as jobs that are either Pending or Running.  As jobs exit, new jobs are
    submitted after a grace period of observation_time seconds.

    Use this profile when you want to maintain a current load on the cluster over a period of time.

    """

    @classmethod
    def add_arguments(cls, sub_parser):
        sub_parser.add_argument("--base_load", type=int, default=5,
                                help="The number of concurrent jobs that should be active at any given time.")

    sub_command_name = "baseload"
    sub_command_help = "Maintains a steady number of jobs"

    def __init__(self):
        self.base_load = 0
        super(BaseLoadProfile, self).__init__()

    def create_jobs(self):
        start_time = self.get_next_start_time()
        logging.debug("Active job count: %s" % self.total_active_jobs)
        if self.total_active_jobs >= self.base_load:
            return

        for i in range(self.total_active_jobs, self.base_load):
            logging.debug("Adding job to submit queue, start time: %s " % start_time)
            cmd = self.create_job_command()
            job_data = {
                'start_time': start_time,
                'job': {
                    'command': cmd,
                    'requested_slots': self.get_num_processors(),
                    'project_name': self.get_project_name(),
                    'queue_name': self.get_queue_name(),
                    'num_tasks': self.get_num_tasks(),
                },
            }

            self.submit_queue.append(job_data)


class SubmitBatchProfile(Profile, object):
    """
    The SubmitBatch profile submits a large number of jobs all at once, then waits for them to complete.  After a some
    observation time period, another batch is submitted, and the process repeats.

    No new jobs are submitted, until all the previous jobs have exited.

    Use this profile when you want to create a peak in demand.

    """

    @classmethod
    def add_arguments(cls, sub_parser):
        sub_parser.add_argument("--min_num_jobs_per_batch", type=int, default=5,
                                help="The minimum number of jobs that should be submitted each time")
        sub_parser.add_argument("--max_num_jobs_per_batch", type=int, default=10,
                                help="The maximum number of jobs that should be submitted each time")
        sub_parser.add_argument("--iterations", type=int, default=0,
                                help="The number of iterations to do before exiting.")

    sub_command_name = "submitbatch"
    sub_command_help = \
        "The SubmitBatch profile submits a large number of jobs all at once, then waits for them to complete."

    def __init__(self):
        self.sum_submitted_batches = 0
        super(SubmitBatchProfile, self).__init__()

    def create_jobs(self):
        start_time = self.get_next_start_time()
        logging.debug("Active job count: %s" % self.total_active_jobs)

        # If there are still active jobs, then don't submit any new ones.
        if self.total_active_jobs > 0:
            return

        if self.iterations != 0 and self.sum_submitted_batches >= self.iterations:
            logging.info("Maximum number of iterations reached, exiting.")
            sys.exit(0)
        self.sum_submitted_batches += 1

        # Number of jobs to submit in this batch.
        num_jobs = randint(self.min_num_jobs_per_batch, self.max_num_jobs_per_batch)
        logging.info("Submitting a batch of %d jobs.")
        for i in range(num_jobs):
            logging.debug("Adding job to submit queue, start time: %s " % start_time)
            job_data = {
                'start_time': start_time,
                'job': {
                    'command': self.create_job_command(),
                    'requested_slots': self.get_num_processors(),
                    'project_name': self.get_project_name(),
                    'queue_name': self.get_queue_name(),
                    'num_tasks': self.get_num_tasks(),
                },
            }

            self.submit_queue.append(job_data)


def get_parser():
    parser = argparse.ArgumentParser(description='Submits load to a batch scheduler')

    parser.add_argument("--failure_rate", type=int, choices=range(0, 101), default=1,
                        help="The percent of submitted jobs that will fail of their own accord at a random interval.")
    parser.add_argument("--office_hours", type=str, default="00:00:00-23:59:59",
                        help="A list of start and end times in the format HH:MM-HH:MM. ")
    parser.add_argument("--min_runtime", type=int, default=600,
                        help=
                        "The minimum amount of time a completed job should run for, in seconds. Default: 10 Minutes.")
    parser.add_argument("--max_runtime", type=int, default=600,
                        help=
                        "The maximum amount of time a completed job should run for, in seconds. Default: 10 Minutes.")
    parser.add_argument("--min_observation_time", type=int, default=120,
                        help="The minimum amount of time a job should be 'observed' before a new job can be submitted.")
    parser.add_argument("--max_observation_time", type=int, default=120,
                        help="The maximum amount of time a job should be 'observed' before a new job can be submitted.")
    parser.add_argument("--min_num_processors", type=int, default=1,
                        help="The minimum number of processors each job should use.  Default 1")
    parser.add_argument("--max_num_processors", type=int, default=1,
                        help="The maximum number of processors each job should use.  Default 1")
    parser.add_argument("--min_tasks_per_job", type=int, default=1,
                        help="The minimum number of tasks per job.  Default  1.")
    parser.add_argument("--max_tasks_per_job", type=int, default=1,
                        help="The maximum number of tasks per job.  Default  1.")

    parser.add_argument("--queue", type=str, dest="queues", action="append", default=[],
                        help="Queue to submit to, if multiple queues are specified, selects one at random.")
    parser.add_argument("--project", dest="projects", type=str, action="append", default=[],
                        help="Project to submit to, if multiple queues are specified, selects one at random.")

    subparsers = parser.add_subparsers(help='sub-command help')
    for cls in Profile.__subclasses__():
        p = subparsers.add_parser(cls.sub_command_name, help=cls.sub_command_help)
        cls.add_arguments(p)
        p.set_defaults(cls=cls)
    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s")
    prs = get_parser()

    # Discover which job manager interfaces are available, set these as
    # choices for the scheduler option.
    #
    # Add any options required by the job manager to the argument parser
    sched_choices = []
    for c in JobManager.__subclasses__():
        c.add_argparse_arguments(prs)
        sched_choices.append(c.scheduler_name)

    prs.add_argument("--scheduler", type=str, dest="scheduler", choices=sched_choices,
                     help="Scheduler interface to use")

    args = prs.parse_args()
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

    # Initialize the job manager, if invalid choice then raise an exception
    manager = None
    for c in JobManager.__subclasses__():
        if c.scheduler_name == args.scheduler:
            manager = c()
            manager.initialize(args)
    if not manager:
        raise ValueError("Manager undefined")

    prof = args.cls()
    prof.manager = manager
    for k, v in vars(args).iteritems():
        setattr(prof, k, v)

    prof.run()
