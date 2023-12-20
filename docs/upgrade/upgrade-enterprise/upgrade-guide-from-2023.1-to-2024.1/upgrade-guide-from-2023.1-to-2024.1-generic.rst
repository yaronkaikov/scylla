.. |SCYLLA_NAME| replace:: ScyllaDB Enterprise

.. |SRC_VERSION| replace:: 2023.1
.. |NEW_VERSION| replace:: 2024.1

.. |DEBIAN_SRC_REPO| replace:: Debian
.. _DEBIAN_SRC_REPO: https://www.scylladb.com/customer-portal/#downloads


.. |UBUNTU_SRC_REPO| replace:: Ubuntu
.. _UBUNTU_SRC_REPO: https://www.scylladb.com/customer-portal/#downloads

.. |SCYLLA_DEB_SRC_REPO| replace:: ScyllaDB deb repo (|DEBIAN_SRC_REPO|_, |UBUNTU_SRC_REPO|_)

.. |SCYLLA_RPM_SRC_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_SRC_REPO: https://www.scylladb.com/customer-portal/#downloads

.. |DEBIAN_NEW_REPO| replace:: Debian
.. _DEBIAN_NEW_REPO: https://www.scylladb.com/customer-portal/?product=ent&platform=debian-9&version=stable-release-2024.1

.. |UBUNTU_NEW_REPO| replace:: Ubuntu
.. _UBUNTU_NEW_REPO: https://www.scylladb.com/customer-portal/?product=ent&platform=ubuntu-20.04&version=stable-release-2024.1

.. |SCYLLA_DEB_NEW_REPO| replace:: ScyllaDB deb repo (|DEBIAN_NEW_REPO|_, |UBUNTU_NEW_REPO|_)

.. |SCYLLA_RPM_NEW_REPO| replace:: ScyllaDB rpm repo
.. _SCYLLA_RPM_NEW_REPO: https://www.scylladb.com/customer-portal/?product=ent&platform=centos7&version=stable-release-2024.1

.. |ROLLBACK| replace:: rollback
.. _ROLLBACK: ./#rollback-procedure

.. |SCYLLA_METRICS| replace:: ScyllaDB Enterprise Metrics Update - ScyllaDB Enterprise 2023.1 to 2024.1
.. _SCYLLA_METRICS: ../metric-update-2023.1-to-2024.1

=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION|
=============================================================================

This document is a step by step procedure for upgrading from |SCYLLA_NAME| 
|SRC_VERSION| to |SCYLLA_NAME| |NEW_VERSION|, and rollback to version 
|SRC_VERSION| if required.

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL) CentOS, 
Debian, and Ubuntu. See :doc:`OS Support by Platform and Version </getting-started/os-support>` 
for information about supported versions.

This guide also applies when you're upgrading ScyllaDB Enterprise official image 
on EC2, GCP, or Azure.

Before You Upgrade ScyllaDB Enterprise
=======================================

**Upgrade Your Driver**

If you're using a :doc:`ScyllaDB driver </using-scylla/drivers/cql-drivers/index>`, 
upgrade the driverbefore you upgrade ScyllaDB Enterprise. The latest two 
versions of each driver are supported.

**Upgrade ScyllaDB Monitoring Stack**

If you're using the ScyllaDB Monitoring Stack, verify that your Monitoring 
Stack version supports  the ScyllaDB Enterprise version to which you want 
to upgrade. See 
`ScyllaDB Monitoring Stack Support Matrix <https://monitoring.docs.scylladb.com/stable/reference/matrix.html>`_.
We recommend upgrading the Monitoring Stack to the latest version.

.. note::

   DateTieredCompactionStrategy is removed in 2024.1. Migrate to 
   TimeWindowCompactionStrategy before you upgrade from 2023.1 to 2024.1.

.. note::

   In ScyllaDB Enterprise 2024.1, Raft-based consistent cluster management for 
   existing deployments is enabled by default. If you want the consistent 
   cluster management feature to be disabled in version 2024.1, you must 
   update the configuration **before** upgrading from 2023.1 to 2024.1:

    #. Set ``consistent_cluster_management: false`` in the ``scylla.yaml`` 
       configuration file on each node in the cluster.
    #. Start the upgrade procedure.

   Consistent cluster management **cannot** be disabled in version 2024.1 if it 
   was enabled in version 2023.1 in one of the following ways:

   * Your cluster was created in version 2023.1 with the default 
     ``consistent_cluster_management: true`` configuration in ``scylla.yaml``.
   * You explicitly set ``consistent_cluster_management: true`` in ``scylla.yaml`` 
     in an existing cluster in version 2023.1.

Upgrade Procedure
=================

A ScyllaDB upgrade is a rolling procedure which does **not** require full cluster shutdown.
For each of the nodes in the cluster you will:

* Check that the cluster's schema is synchronized
* Drain the node and backup the data
* Backup the configuration file
* Stop ScyllaDB
* Download and install new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful


.. caution:: 

   Apply the procedure **serially** on each node. Do not move to the next node before validating that the node you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use the new |NEW_VERSION| features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes. See `sctool <https://manager.docs.scylladb.com/stable/sctool/>`_ for suspending ScyllaDB Manager's scheduled or running repairs.
* Not to apply schema changes.

**After** the upgrade, you need to verify that Raft was successfully initiated 
in your cluster. You can skip this step only in one of the following cases:

* The ``consistent_cluster_management`` option was enabled in a previous 
  ScyllaDB version.
* You you disabled the ``consistent_cluster_management`` option before 
  upgrading to version 2024.1, as described in the note in the *Before 
  You Upgrade ScyllaDB* section.

Otherwise, as soon as every node has been upgraded to the new version, 
the cluster will start a procedure that initializes the Raft algorithm for 
consistent cluster metadata management. You must then 
:ref:`verify <validate-raft-setup-enabled-default-2024.1>` that the Raft 
initialization procedure has successfully finished.


Upgrade Steps
=============
Check the cluster schema
-------------------------
Make sure that all nodes have the schema synchronized before upgrade. The upgrade procedure will fail if there is a schema disagreement between nodes.

.. code:: sh

   nodetool describecluster

Backup the data
-----------------------------------
Before any major procedure, like an upgrade, it is recommended to backup all the data to an external device.
You can use `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/backup/index.html>`_ for creating backups.

Backup the configuration file
------------------------------
.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

.. _upgrade-debian-ubuntu-enterprise-2024.1: 

Download and install the new release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        Before upgrading, check what version you are running now using ``scylla --version``. You should use the same version as this version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

        **To upgrade ScyllaDB Enterprise:**

        #. Update the |SCYLLA_DEB_NEW_REPO| to |NEW_VERSION| and and enable scylla/ppa repo:

            .. code-block:: console

               sudo add-apt-repository -y ppa:scylladb/ppa

        #. Configure Java 1.8:

            .. code-block:: console

               sudo apt-get update
               sudo apt-get install -y openjdk-8-jre-headless
               sudo update-java-alternatives -s java-1.8.0-openjdk-amd64


        #. Install the new ScyllaDB version:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla-enterprise


        Answer ‘y’ to the first two questions.


        **Installing the New Version on Cloud**
        
        If you're using the ScyllaDB official image (recommended), see the **EC2/GCP/Azure Ubuntu Image** tab for upgrade instructions.
        If you're using your own image and installed ScyllaDB packages for Ubuntu or Debian, you need to apply an extended upgrade 
        procedure:

        #. Update the ScyllaDB deb repo (see above).
        #. Configure Java 1.8 (see above).
        #. Install the new ScyllaDB version with the additional ``scylla-enterprise-machine-image`` package:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla-enterprise
               sudo apt-get dist-upgrade scylla-enterprise-machine-image

        #. Run ``scylla_setup`` without ``running io_setup``.
        #. Run ``sudo /opt/scylladb/scylla-machine-image/scylla_cloud_io_setup``.


   .. group-tab:: RHEL/CentOS

        Before upgrading, check what version you are running now using ``scylla --version``. You should use the same version as this version in case you want to |ROLLBACK|_ the upgrade. If you are not running a |SRC_VERSION|.x version, stop right here! This guide only covers |SRC_VERSION|.x to |NEW_VERSION|.y upgrades.

        **To upgrade ScyllaDB:**

        #. Update the |SCYLLA_RPM_NEW_REPO|_  to |NEW_VERSION|.
        #. Install the new ScyllaDB version:

            .. code:: sh

               sudo yum clean all
               sudo yum update scylla\* -y

        **Installing the New Version on Cloud**
        
        If you're using the ScyllaDB official image (recommended), see the **EC2/GCP/Azure Ubuntu Image** tab for upgrade instructions.
        If you're using your own image and installed ScyllaDB packages for CentOS/RHEL, you need to apply an extended upgrade 
        procedure:

        #. Update the ScyllaDB deb repo (see above).
        #. Install the new ScyllaDB version with the additional ``scylla-enterprise-machine-image`` package:

            .. code-block:: console

               sudo yum clean all
               sudo yum update scylla\* -y
               sudo yum update scylla-enterprise-machine-image

        #. Run ``scylla_setup`` without ``running io_setup``.
        #. Run ``sudo /opt/scylladb/scylla-machine-image/scylla_cloud_io_setup``.

.. note::

   If you are running a ScyllaDB Enterprise official image (for EC2 AMI, GCP, or Azure), you need to 
   download and install the new ScyllaDB Enterprise release for Ubuntu. 
   See :doc:`Upgrade ScyllaDB Image </upgrade/ami-upgrade>` for more information.

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
#. Check cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in ``UN`` status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB version. Validate that the version matches the one you upgraded to.
#. Check scylla-server log (using ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no new errors in the log.
#. Check again after two minutes, to validate no new issues are introduced.

Once you are sure the node upgrade was successful, move to the next node in the cluster.

See |Scylla_METRICS|_ for more information.

After Upgrading Every Node
===============================

This section applies to upgrades where Raft is initialized for the first time 
in the cluster, which in 2024.1 happens by default.

You can skip this section only in one of the following cases:

* The ``consistent_cluster_management`` option was enabled in a previous 
  ScyllaDB version (i.e., Raft was enabled in a version prior to 2024.1).
* You disabled the ``consistent_cluster_management`` option before upgrading 
  to 2024.1, as described in the note in the *Before You Upgrade ScyllaDB* 
  section (i.e., you prevented Raft from being enabled in 2024.1).

.. _validate-raft-setup-enabled-default-2024.1:

Validate Raft Setup
-------------------------

Enabling Raft causes the ScyllaDB cluster to start an internal Raft 
initialization procedure as soon as every node is upgraded to the new version. 
The goal of that procedure is to initialize data structures used by the Raft 
algorithm to consistently manage cluster-wide metadata, such as table schemas.

Assuming you performed the rolling upgrade procedure correctly (in particular, 
ensuring that the schema is synchronized on every step), and if there are no 
problems with cluster connectivity, that internal procedure should take a few 
seconds to finish. However, the procedure requires full cluster availability.
If one of the nodes fails before the procedure finishes (for example, due to 
a hardware problem), the process may get stuck, which may prevent schema or 
topology changes in your cluster.

Therefore, following the rolling upgrade, you must verify that the internal 
Raft initialization procedure has finished successfully by checking the logs 
of every ScyllaDB node. If the process gets stuck, manual intervention is 
required.

Refer to the 
:ref:`Verifying that the internal Raft upgrade procedure finished successfully <verify-raft-procedure>` 
section for instructions on verifying that the procedure was successful and 
proceeding if it gets stuck.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from |SCYLLA_NAME| |NEW_VERSION|.x to |SRC_VERSION|.y. Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes. Use this procedure only for nodes you upgraded to |NEW_VERSION|.

.. warning::

   The rollback procedure can be applied **only** if some nodes have not been upgraded to |NEW_VERSION| yet.
   As soon as the last node in the rolling upgrade procedure is started with |NEW_VERSION|, rollback becomes impossible.
   At that point, the only way to restore a cluster to |SRC_VERSION| is by restoring it from backup.

ScyllaDB rollback is a rolling procedure which does **not** require a full cluster shutdown.
For each of the nodes you rollback to |SRC_VERSION| you will:

* Drain the node and stop ScyllaDB
* Retrieve the old ScyllaDB packages
* Restore the configuration file
* Restore system tables
* Reload systemd configuration
* Restart ScyllaDB
* Validate the rollback success

Apply the following procedure **serially** on each node. Do not move to the next node before validating that the rollback was successful and the node is up and running the old version.

Rollback Steps
==============
Drain and gracefully stop the node
----------------------------------

.. code:: sh

   nodetool drain
   sudo service scylla-server stop

Download and install the old release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        #. Remove the old repo file.

            .. code:: sh

               sudo rm -rf /etc/apt/sources.list.d/scylla.list

        #. Update the |SCYLLA_DEB_SRC_REPO| to |SRC_VERSION|.
        #. Install:

            .. code-block::

               sudo apt-get update
               sudo apt-get remove scylla\* -y
               sudo apt-get install scylla-enterprise

        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

        #. Remove the old repo file.

            .. code:: sh

               sudo rm -rf /etc/yum.repos.d/scylla.repo

        #. Update the |SCYLLA_RPM_SRC_REPO|_  to |SRC_VERSION|.
        #. Install:

            .. code:: console

               sudo yum clean all
               sudo rm -rf /var/cache/yum
               sudo yum remove scylla\*tools-core
               sudo yum downgrade scylla\* -y
               sudo yum install scylla-enterprise

.. note::

   If you are running a ScyllaDB Enterprise official image (for EC2 AMI, GCP, 
   or Azure), follow the instructions for Ubuntu. 


Restore the configuration file
------------------------------
.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-src | /etc/scylla/scylla.yaml

Reload systemd configuration
----------------------------

You must reload the unit file if the systemd unit file is changed.

.. code:: sh

   sudo systemctl daemon-reload

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.