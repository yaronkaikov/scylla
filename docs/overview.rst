:orphan:

====================================
ScyllaDB Enterprise Documentation
====================================

.. meta::
   :title: ScyllaDB Enterprise Documentation
   :description: ScyllaDB Enterprise Documentation
   :keywords: ScyllaDB Enterprise, Scylla Enterprise, ScyllaDB Enterprise documentation, ScyllaDB support, ScyllaDB Enterprise features, Scylla Enterprise features

About This User Guide
-----------------------

ScyllaDB Enterprise is a distributed NoSQL wide-column database for data-intensive apps that require 
high performance and low latency. It provides the functionality of ScyllaDB Open Source, with the addition of
:ref:`Enterprise-only features <landing-page-enterprise-features>` and :ref:`24/7 support <landing-page-enterprise-support>`.

You can deploy ScyllaDB Enterprise:

* On AWS, GCP, and Azure.
* On premises.
* With `ScyllaDB Cloud <https://cloud.scylladb.com/>`_ - a fully managed DBaaS. 

This user guide covers topics related to ScyllaDB Enterprise cloud or on-premises self-managed deployments.

For details about ScyllaDB Cloud, see the `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_.


Documentation Highlights
--------------------------

* :doc:`Install ScyllaDB Enterprise </getting-started/install-scylla/index>`
* :doc:`Configure ScyllaDB Enterprise </getting-started/system-configuration/>`
* :doc:`Cluster Management Procedures </operating-scylla/procedures/cluster-management/index>`
* :doc:`ScyllaDB Enterprise Upgrade </upgrade/index>`
* :doc:`CQL Reference </cql/index>`
* :doc:`ScyllaDB Drivers </using-scylla/drivers/index>`

.. _landing-page-enterprise-features:

Enterprise-only  Features
------------------------------

ScyllaDB Enterprise comes with a number of features that are not available in ScyllaDB Open Source, including:

* :doc:`Encryption at Rest </operating-scylla/security/encryption-at-rest>`
* :doc:`Workload Prioritization </using-scylla/workload-prioritization>`
* :doc:`LDAP Authorization </operating-scylla/security/ldap-authorization>` and :doc:`LDAP Authentication </operating-scylla/security/ldap-authentication>`
* :doc:`Audit </operating-scylla/security/auditing>`
* :ref:`Incremental Compaction Strategy (ICS) <incremental-compaction-strategy-ics>`

.. _landing-page-enterprise-support:

ScyllaDB Enterprise Support
-----------------------------

ScyllaDB customers can open or check on tickets in the `ScyllaDB Customer Portal <https://www.scylladb.com/customer-portal/#support>`_.

In addition, the Customer Portal allows you to download ScyllaDB Enterprise and learn about premium training options.


Learn How to Use ScyllaDB
---------------------------

You can learn to use ScyllaDB by taking free courses at `ScyllaDB University <https://university.scylladb.com/>`_. Also, you can read our `blog <https://www.scylladb.com/blog/>`_ and attend ScyllaDB's `webinars, workshops, and conferences <https://www.scylladb.com/company/events/>`_.

.. toctree::
  :hidden:

  getting-started/index
  operating-scylla/index
  using-scylla/index
  cql/index
  architecture/index
  troubleshooting/index
  kb/index
  ScyllaDB University <https://university.scylladb.com/>
  faq
  Contribute to ScyllaDB <contribute>
  glossary
  alternator/alternator
