# ScyllaDB Enterprise Documentation

Welcome to the ScyllaDB Enterprise documentation repository. Here, you will find the source files that make up the documentation for the ScyllaDB Enterprise edition.

## Docs structure and maintenance

This repository is regularly updated with backports from the ScyllaDB Open Source repository. Due to this relationship, there are some configuration differences that should be noted:

* This project's index page and table of contents are contained within the `overview.rst` file instead of using the default ``index.rst`` file.
* This project's Sphinx configuration file is in the` _enterprise` folder. 

This separation is critical to avoid overwriting the files in the root folder during the backport process.

## Building the docs

To build the documentation, execute the following command:


```console
make FLAG=enterprise preview.
```

**IMPORTANT:** Remember to include the `FLAG=enterprise` argument for building the enterprise version of the documentation.


## Displaying Enterprise specific content

You can control the visibility of content based on the version (`opensource` or `enterprise`) being built by using the `only` directive in RST files.
Here's an example:

```rst
.. only:: enterprise

   This content is displayed exclusively for Scylla Enterprise.

.. only:: opensource

   This content is displayed exclusively for Scylla Open Source.
```

