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

  New to ScyllaDB? Start `here <https://cloud.docs.scylladb.com/stable/scylladb-basics/>`_!

You can deploy ScyllaDB Enterprise:

* On AWS, GCP, and Azure.
* On premises.
* With `ScyllaDB Cloud <https://cloud.scylladb.com/>`_ - a fully managed DBaaS. 

This user guide covers topics related to ScyllaDB Enterprise cloud or on-premises self-managed deployments.

For details about ScyllaDB Cloud, see the `ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/>`_.


  <div class="topics-grid topics-grid--scrollable grid-container full">

  <div class="grid-x grid-margin-x hs">

.. topic-box::
  :title: ScyllaDB Cloud
  :link: https://cloud.docs.scylladb.com
  :class: large-4
  :anchor: ScyllaDB Cloud Documentation

* :doc:`Install ScyllaDB Enterprise </getting-started/install-scylla/index>`
* :doc:`Configure ScyllaDB Enterprise </getting-started/system-configuration/>`
* :doc:`Cluster Management Procedures </operating-scylla/procedures/cluster-management/index>`
* :doc:`ScyllaDB Enterprise Upgrade </upgrade/index>`
* :doc:`CQL Reference </cql/index>`
* :doc:`ScyllaDB Drivers </using-scylla/drivers/index>`

.. topic-box::
  :title: ScyllaDB Enterprise
  :link: https://enterprise.docs.scylladb.com
  :class: large-4
  :anchor: ScyllaDB Enterprise Documentation

  Deploy and manage ScyllaDB's most stable enterprise-grade database with premium features and 24/7 support.

.. topic-box::
  :title: ScyllaDB Open Source
  :link: getting-started
  :class: large-4
  :anchor: ScyllaDB Open Source Documentation

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
  Deploy and manage your database in your environment.


Learn How to Use ScyllaDB
---------------------------

You can learn to use ScyllaDB by taking free courses at `ScyllaDB University <https://university.scylladb.com/>`_. Also, you can read our `blog <https://www.scylladb.com/blog/>`_ and attend ScyllaDB's `webinars, workshops, and conferences <https://www.scylladb.com/company/events/>`_.

=======
.. raw:: html

  <div class="topics-grid topics-grid--products">

      <h2 class="topics-grid__title">Other Products</h2>

      <div class="grid-container full">
          <div class="grid-x grid-margin-x">

.. topic-box::
  :title: ScyllaDB Alternator
  :link: https://docs.scylladb.com/stable/alternator/alternator.html
  :image: /_static/img/mascots/scylla-alternator.svg
  :class: topic-box--product,large-4,small-6

  Open source Amazon DynamoDB-compatible API.

.. topic-box::
  :title: ScyllaDB Monitoring Stack
  :link: https://monitoring.docs.scylladb.com
  :image: /_static/img/mascots/scylla-monitor.svg
  :class: topic-box--product,large-4,small-6

  Complete open source monitoring solution for your ScyllaDB clusters.

.. topic-box::
  :title: ScyllaDB Manager
  :link: https://manager.docs.scylladb.com
  :image: /_static/img/mascots/scylla-manager.svg
  :class: topic-box--product,large-4,small-6

  Hassle-free ScyllaDB NoSQL database management for scale-out clusters.

.. topic-box::
  :title: ScyllaDB Drivers
  :link: https://docs.scylladb.com/stable/using-scylla/drivers/
  :image: /_static/img/mascots/scylla-drivers.svg
  :class: topic-box--product,large-4,small-6

  Shard-aware drivers for superior performance. 

.. topic-box::
  :title: ScyllaDB Operator
  :link: https://operator.docs.scylladb.com
  :image: /_static/img/mascots/scylla-enterprise.svg
  :class: topic-box--product,large-4,small-6

  Easily run and manage your ScyllaDB cluster on Kubernetes.

.. raw:: html

  </div></div></div>

.. raw:: html

  <div class="topics-grid">

      <h2 class="topics-grid__title">Learn More About ScyllaDB</h2>
      <p class="topics-grid__text"></p>
      <div class="grid-container full">
          <div class="grid-x grid-margin-x">

.. topic-box::
  :title: Attend ScyllaDB University
  :link: https://university.scylladb.com/
  :image: /_static/img/mascots/scylla-university.png
  :class: large-6,small-12
  :anchor: Find a Class

  | Register to take a *free* class at ScyllaDB University.
  | There are several learning paths to choose from.

.. topic-box::
  :title: Register for a Webinar
  :link: https://www.scylladb.com/resources/webinars/
  :image: /_static/img/mascots/scylla-with-computer-2.png
  :class: large-6,small-12
  :anchor: Find a Webinar

  | You can either participate in a live webinar or see a recording on demand.
  | There are several webinars to choose from.

.. raw:: html

  </div></div></div>

.. raw:: html

  </div>

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

