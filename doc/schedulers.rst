.. LavaStorm documentation master file, created by
   sphinx-quickstart on Sat Aug 16 21:59:30 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Scheduler Support
=================

LavaStorm can talk to a number of schedulers including OpenLava, (Using the command line, C API, or through openlava web)
and Sun Grid Engine.  Adding support for new schedulers is easy.  Schedulers are implemented using two components:
SimpleJob - a very simple class that represents an active job, implements a method to kill the job, and provides
attributes that denote the status of the job and its IDs; and JobManager - the actual interface to the scheduler that
provides methods to retrieve a job or jobs, and submit new jobs into the batch scheduling system.

OpenLava
========

CLI
---

.. autoclass:: lavaStorm.DirectOpenLavaManager

.. autoclass:: lavaStorm.OpenLavaDirectJob

Cluster API
-----------

.. autoclass:: lavaStorm.OpenLavaClusterAPIManager

Openlava Web
------------

.. autoclass:: lavaStorm.OpenLavaRemoteManager

C API
-----

.. autoclass:: lavaStorm.OpenLavaCAPIManager

Sun Grid Engine
===============

.. autoclass:: lavaStorm.DirectSGEManager
