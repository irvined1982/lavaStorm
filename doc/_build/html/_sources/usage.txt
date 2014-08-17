LavaStorm Usage
===============
::

    usage: lavaStorm.py [-h]
                        [--failure_rate FAILURE_RATE]
                        [--office_hours OFFICE_HOURS]
                        [--min_runtime MIN_RUNTIME]
                        [--max_runtime MAX_RUNTIME]
                        [--min_observation_time MIN_OBSERVATION_TIME]
                        [--max_observation_time MAX_OBSERVATION_TIME]
                        [--min_num_processors MIN_NUM_PROCESSORS]
                        [--max_num_processors MAX_NUM_PROCESSORS]
                        [--min_tasks_per_job MIN_TASKS_PER_JOB]
                        [--max_tasks_per_job MAX_TASKS_PER_JOB]
                        [--queue QUEUES]
                        [--project PROJECTS]
                        [--url URL]
                        [--username USERNAME]
                        [--password PASSWORD]
                        {baseload|submitbatch} ...

    Submits load to a batch scheduler

    positional arguments:
      {baseload,submitbatch}
                            sub-command help
        baseload            Maintains a steady number of jobs
        submitbatch         The SubmitBatch profile submits a large number of jobs
                            all at once, then waits for them to complete.



.. automodule:: lavaStorm
