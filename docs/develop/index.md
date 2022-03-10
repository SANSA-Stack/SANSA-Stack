---
title: Source Code
has_children: false
nav_order: 1
---

The Sansa project comprises java/scala libraries, a command line tool and examples.

A Makefile exists in the project root folder which helps achieving certain goals efficiently by hiding arcance maven invocations behind the following make goals:

```bash
➜ make
make help                # Show these help instructions
make mcis                # mvn skip clean install (minimal build of all modules) - Passing args:  make mcis ARGS=-X
make rebuild-examples    # rebuild and shade examples - they must be present for integration tests
make shade-examples      # only shade the examples - use after manual rebuild of specific modules
make integration-tests   # run the integration tests
make deb-rebuild         # rebuild the deb package (minimal build of only required modules)
make deb-reinstall       # reinstall a previously built deb package
make rpm-rebuild         # rebuild the rpm package (minimal build of only required modules
make rpm-reinstall       # reinstall a previously built rpm package

```


### Integration Tests

The integration test (IT) setup starts a dockerized spark cluster (via testcontainers) comprising containers for running
* the master
* the two worker nodes
* the spark submit command

The test submits the the shaded examples jar which must have been built using `rebuild-examples`

The following components are part of the IT:

* sparqlify sparql server
* ontop sparql server

