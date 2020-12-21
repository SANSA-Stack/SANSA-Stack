## SANSA Integration Tests

This module contains integration tests (IT) based on docker and docker-compose via [TestContainers](https://www.testcontainers.org/).
Running the IT thus require a working docker environment.

The following ports of the docker containers are used and possibly exposed on the host.
Note, that the IT setup is still work-in-progress and the port mapping and port exposition may change.

* 7541 - spark-web-ui
* 7542 - spark-master
* 7543-7548 - reserved for workers
* 7549 - application port (e.g. sparklify / ontop)


## Running the Integration Tests
The IT requires the sansa-examples-spark jar bundle to have been built:

```
# All commands are expected to be run from the  root folder of the SANSA git repository

# Build the whole project once if this hasn't been done yt
mvn clean install -DskipTests

# Create the jar bundle for the sansa-examples-spark only (building all bundles takes very long)
mvn -pl sansa-examples/sansa-examples-spark -Pdist package

# Run the integration test
mvn -pl sansa-integration-tests failsafe:integration-test

```


