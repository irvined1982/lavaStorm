Extending LavaStorm
===================

LavaStorm can be extended easily by subclassing the Profile class.  Once you create your own class, set the class
attribute sub_command_name and sub_command_help.  The former defines the name of the profile argument, and should be
a single word, short, and describe the profile.  The second is a verbose help field that explains something about that
profile, this will be displayed when the LavaStorm is called with the --help argument.

Generally, you only need to re-implement one other method: create_jobs().  This method must decide how many jobs are
needed and when they should be submitted to the scheduler.  Once put in the submit_queue, all other operations are
automatic.

Job Creation Process
--------------------

Jobs are put in the submit_queue by appending a dictionary with two keys, start_time, a datetime object set to a date
before which the job should not be submitted to the cluster.  The second key, job, is a dictionary of arguments that
should be passed to the scheduler to execute the job.  There are helper functions that will populate the data based
on the range supplied by the user.::

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

Process API
-----------

Class Attributes
^^^^^^^^^^^^^^^^

.. autoattribute:: lavaStorm.Profile.sub_command_name

.. autoattribute:: lavaStorm.Profile.sub_command_help

Helper Attributes
^^^^^^^^^^^^^^^^^

.. py:attribute:: lavaStorm.Profile.total_submitted_jobs

The total number of jobs that have been submitted to the cluster.

.. py:attribute:: lavaStorm.Profile.total_active_jobs = 0

The total number of active jobs that are on the cluster.  This does not include jobs that are waiting to be submitted.

.. py:attribute:: lavaStorm.Profile.total_finished_jobs = 0

The total number of jobs that have finished, this includes jobs that have failed or killed tasks.

.. py:attribute:: lavaStorm.Profile.total_task_count = 0

The total number of tasks for all jobs.

.. py:attribute:: lavaStorm.Profile.pending_task_count = 0

The total number of tasks that have been submitted into the cluster and are in a pending state.

.. py:attribute:: lavaStorm.Profile.running_task_count = 0

The total number of tasks that are currently executing on the cluster.

.. py:attribute:: lavaStorm.Profile.killed_task_count = 0

The total number of tasks that have been killed.

.. py:attribute:: lavaStorm.Profile.completed_task_count = 0

The total number of tasks that have completed successfully.

.. py:attribute:: lavaStorm.Profile.failed_task_count = 0

The total number of tasks that have failed.

Helper Methods
^^^^^^^^^^^^^^

The following methods simplify creating job parameters and making decisions about submitting new jobs.

.. automethod:: lavaStorm.Profile.is_active

.. automethod:: lavaStorm.Profile.get_queue_name

.. automethod:: lavaStorm.Profile.get_project_name

.. automethod:: lavaStorm.Profile.get_num_tasks

.. automethod:: lavaStorm.Profile.get_num_processors

.. automethod:: lavaStorm.Profile.get_runtime_seconds

.. automethod:: lavaStorm.Profile.get_observation_time

.. automethod:: lavaStorm.Profile.get_next_start_time

.. automethod:: lavaStorm.Profile.create_job_command

Core Methods
^^^^^^^^^^^^

The following methods form the core, and can be overridden as required.  Usually only add_arguments and create_jobs will
need to be modified.

.. automethod:: lavaStorm.Profile.add_arguments

.. automethod:: lavaStorm.Profile.create_jobs

.. automethod:: lavaStorm.Profile.start_job

.. automethod:: lavaStorm.Profile.start_jobs

.. automethod:: lavaStorm.Profile.get_job

.. automethod:: lavaStorm.Profile.get_jobs

.. automethod:: lavaStorm.Profile.process_running_jobs

.. automethod:: lavaStorm.Profile.run

.. automethod:: lavaStorm.Profile.start_jobs

.. automethod:: lavaStorm.Profile.kill_all_jobs
