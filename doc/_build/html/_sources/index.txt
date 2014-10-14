.. LavaStorm documentation master file, created by
   sphinx-quickstart on Sat Aug 16 21:59:30 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

LavaStorm
=========

LavaStorm automates job submission and resource consumption for testing and validation of the HPC scheduling systems.
LavaStorm submits an arbitrary number of jobs, selecting projects, queues, job size, and run time within predefined
limits set by the user.

The submitted jobs execute and consume resources for the time specified before exiting. This is ideal when you want to
check that the cluster is functioning normally before handing it over for end user access or when you wish to evaluate
changes to the scheduling environment.

Various profiles are available to mimic common user behavior, and it is easy to create additional profiles, or modify
the existing ones to match the desired profile.

LavaStorm can talk to a number of schedulers including OpenLava, (Using the command line, C API, or through openlava web)
and Sun Grid Engine.  Adding support for new schedulers is easy.
Contents:

.. toctree::
    :maxdepth: 2

    profiles
    schedulers
    usage
    extension

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

