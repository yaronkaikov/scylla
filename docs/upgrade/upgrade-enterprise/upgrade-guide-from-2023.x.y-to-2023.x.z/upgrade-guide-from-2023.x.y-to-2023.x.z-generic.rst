.. |SCYLLA_NAME| replace:: ScyllaDB Enterprise

.. |SRC_VERSION| replace:: 2023.x.y
.. |NEW_VERSION| replace:: 2023.x.z

.. |MINOR_VERSION| replace:: 2023.x

.. |SCYLLA_DEB_NEW_REPO| replace:: ScyllaDB Enterprise deb repo
.. _SCYLLA_DEB_NEW_REPO: https://www.scylladb.com/download/#enterprise

.. |SCYLLA_RPM_NEW_REPO| replace:: ScyllaDB Enterprise rpm repo
.. _SCYLLA_RPM_NEW_REPO: https://www.scylladb.com/download/#enterprise

=============================================================================
Upgrade Guide - |SCYLLA_NAME| |SRC_VERSION| to |NEW_VERSION|
=============================================================================

This document is a step-by-step procedure for upgrading from |SCYLLA_NAME| |SRC_VERSION| 
to |SCYLLA_NAME| |NEW_VERSION| (where "z" is the :ref:`latest available version <faq-pinning>`).

Applicable Versions
===================

This guide covers upgrading ScyllaDB on Red Hat Enterprise Linux (RHEL), CentOS, Debian, 
and Ubuntu. See :doc:`OS Support by Platform and Version </getting-started/os-support>` for 
information about supported versions.

This guide also applies when you're upgrading ScyllaDB Enterprise official image on EC2, GCP, 
or Azure.

Upgrade Procedure
=================

A ScyllaDB Enterprise upgrade is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes in the cluster, you will:

* Drain the node and backup the data
* Check your current release
* Backup the configuration file
* Stop ScyllaDB
* Download and install the new ScyllaDB packages
* Start ScyllaDB
* Validate that the upgrade was successful

.. caution:: 
    
    Apply the following procedure **serially** on each node. Do not move to the next node before validating 
    the node that you upgraded is up and running the new version.

**During** the rolling upgrade, it is highly recommended:

* Not to use new |NEW_VERSION| features.
* Not to run administration functions, like repairs, refresh, rebuild or add or remove nodes.
* Not to apply schema changes.

Upgrade Steps
==============

Drain the node and backup the data
------------------------------------

Before any major procedure, such as an upgrade, it is recommended to backup all the data to an external device.
You can use `ScyllaDB Manager <https://manager.docs.scylladb.com/stable/backup/index.html>`_ for creating backups.

Backup the configuration file
-------------------------------

.. code:: sh

   sudo cp -a /etc/scylla/scylla.yaml /etc/scylla/scylla.yaml.backup-src

Backup more config files.

.. code:: sh

   for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ); do sudo cp -v $conf $conf.backup-2.1; done

Gracefully stop the node
------------------------

.. code:: sh

   sudo service scylla-server stop

Download and install the new release
------------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        Before upgrading, check what version you are running now using ``scylla --version``. You should use 
        the same version in case you want to rollback the upgrade.

        **To upgrade ScyllaDB Enterprise:**
        
        #. Update the |SCYLLA_DEB_NEW_REPO|_ to |MINOR_VERSION|.
        #. Install:
        
            .. code:: sh

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla-enterprise

            Answer ‘y’ to the first two questions.


        **Installing the New Version on Cloud**
        
        If you're using the ScyllaDB official image (recommended), see the **EC2/GCP/Azure Ubuntu Image** tab
        for upgrade instructions.
        If you're using your own image and installed ScyllaDB packages for Ubuntu or Debian, you need to apply 
        an extended upgrade procedure:

        #. Update the ScyllaDB Enterprise deb repo (see above).
        #. Install the new ScyllaDB version with the additional ``scylla-enterprise-machine-image`` package:

            .. code-block:: console

               sudo apt-get clean all
               sudo apt-get update
               sudo apt-get dist-upgrade scylla-enterprise
               sudo apt-get dist-upgrade scylla-enterprise-machine-image

        #. Run ``scylla_setup`` without ``running io_setup``.
        #. Run ``sudo /opt/scylladb/scylla-machine-image/scylla_cloud_io_setup``.

   .. group-tab:: RHEL/CentOS

        Before upgrading, check what version you are running now using ``scylla --version``. You should use 
        the same version in case you want to rollback the upgrade.

        To upgrade:

        #. Update the |SCYLLA_RPM_NEW_REPO|_ to |MINOR_VERSION|.
        #. Install:

            .. code:: sh

               sudo yum clean all
               sudo yum update scylla\* -y

   .. group-tab:: EC2/GCP/Azure Ubuntu Image

        Before upgrading, check what version you are running now using ``scylla --version``. You should use 
        the same version in case you want to rollback the upgrade.

        There are two alternative upgrade procedures:

        * :ref:`Upgrading ScyllaDB and simultaneously updating 3rd party and OS packages <upgrade-image-recommended-procedure-enterprise-2023.x.z>`. It is recommended if you are running a ScyllaDB official image (EC2 AMI, GCP, and Azure images).

        * Upgrading ScyllaDB without updating any external packages - follow the instructions in the *Debian/Ubuntu* tab.

        .. _upgrade-image-recommended-procedure-enterprise-2023.x.z:

        **To upgrade ScyllaDB Enterprise and update 3rd party and OS packages (RECOMMENDED):**

        Choosing this upgrade procedure allows you to upgrade your ScyllaDB version and update the 3rd party 
        and OS packages using one command. 

        #. Update the |SCYLLA_DEB_NEW_REPO|_ to |MINOR_VERSION|.

        #. Load the new repo:

            .. code:: sh 
    
                 sudo apt-get update

        #. Run the following command to update the manifest file:
    
            .. code:: sh 
    
               cat scylla-enterprise-packages-<version>-<arch>.txt | sudo xargs -n1 apt-get install -y
    
        Where:

        * ``<version>`` - The ScyllaDB Enterprise version to which you are upgrading ( |NEW_VERSION| ).
        * ``<arch>`` - Architecture type: ``x86_64`` or ``aarch64``.
    
        The file is included in the ScyllaDB Enterprise packages downloaded in the previous step. The file location is 
        ``http://downloads.scylladb.com/downloads/scylla-enterprise/aws/manifest/scylla-enterprise-packages-<version>-<arch>.txt``.

        Example:
    
            .. code:: console 
           
               cat scylla-enterprise-packages-2023.1.1-x86_64.txt | sudo xargs -n1 apt-get install -y


            .. note:: 

               Alternatively, you can update the manifest file with the following command:

               ``sudo apt-get install $(awk '{print $1'} scylla-enterprise-packages-<version>-<arch>.txt) -y``

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
#. Check the cluster status with ``nodetool status`` and make sure **all** nodes, including the one you just upgraded, are in UN status.
#. Use ``curl -X GET "http://localhost:10000/storage_service/scylla_release_version"`` to check the ScyllaDB Enterprise version.
#. Check scylla-server log (by ``journalctl _COMM=scylla``) and ``/var/log/syslog`` to validate there are no errors.
#. Check again after 2 minutes to validate no new issues are introduced.

Once you are sure the node upgrade is successful, move to the next node in the cluster.

Rollback Procedure
==================

.. include:: /upgrade/_common/warning_rollback.rst

The following procedure describes a rollback from ScyllaDB Enterprise |NEW_VERSION| to |SRC_VERSION|. 
Apply this procedure if an upgrade from |SRC_VERSION| to |NEW_VERSION| failed before completing on all nodes.

ScyllaDB rollback is a rolling procedure that does **not** require a full cluster shutdown.
For each of the nodes to rollback to |SRC_VERSION|, you will:

* Gracefully shutdown ScyllaDB
* Downgrade to the previous release
* Restore the configuration file
* Restart ScyllaDB
* Validate the rollback success

.. caution:: 
    
    Apply the following procedure **serially** on each node. Do not move to the next node before validating 
    the node that you upgraded is up and running the new version.

Rollback Steps
==============

Gracefully shutdown ScyllaDB
-----------------------------

.. code:: sh

   nodetool drain
   sudo service scylla-server stop

Downgrade to the previous release
----------------------------------

.. tabs::

   .. group-tab:: Debian/Ubuntu

        Install:

            .. code-block:: console
               :substitutions:

               sudo apt-get install scylla-enterprise=|SRC_VERSION|\* scylla-enterprise-server=|SRC_VERSION|\* scylla-enterprise-jmx=|SRC_VERSION|\* scylla-enterprise-tools=|SRC_VERSION|\* scylla-enterprise-tools-core=|SRC_VERSION|\* scylla-enterprise-kernel-conf=|SRC_VERSION|\* scylla-enterprise-conf=|SRC_VERSION|\* scylla-enterprise-python3=|SRC_VERSION|\*
               sudo apt-get install scylla-enterprise-machine-image=|SRC_VERSION|\*  # only execute on AMI instance


        Answer ‘y’ to the first two questions.

   .. group-tab:: RHEL/CentOS

       Install:

        .. code-block:: console
           :substitutions:

            sudo yum downgrade scylla\*-|SRC_VERSION|-\* -y


Restore the configuration file
------------------------------

.. code:: sh

   sudo rm -rf /etc/scylla/scylla.yaml
   sudo cp -a /etc/scylla/scylla.yaml.backup-src /etc/scylla/scylla.yaml

Restore more config files.

.. code:: sh

   for conf in $(cat /var/lib/dpkg/info/scylla-*server.conffiles /var/lib/dpkg/info/scylla-*conf.conffiles /var/lib/dpkg/info/scylla-*jmx.conffiles | grep -v init ); do sudo cp -v $conf.backup-2.1 $conf; done
   sudo systemctl daemon-reload

Start the node
--------------

.. code:: sh

   sudo service scylla-server start

Validate
--------
Check the upgrade instructions above for validation. Once you are sure the node rollback is successful, move to the next node in the cluster.

