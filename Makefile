CWD = $(shell pwd)

POM = -f sansa-bom/pom.xml

# Maven Clean Install Skip ; skip tests, javadoc, scaladoc, etc
MS = mvn -DskipTests -Dmaven.javadoc.skip=true -Dskip
MCCS = $(MS) clean compile
MCIS = $(MS) clean install

# Source: https://stackoverflow.com/questions/4219255/how-do-you-get-the-list-of-targets-in-a-makefile
.PHONY: help
help:  ## Show these help instructions
	@sed -rn 's/^([a-zA-Z_-]+):.*?## (.*)$$/"\1" "\2"/p' < $(MAKEFILE_LIST) | xargs printf "make %-20s# %s\n"

mcis: ## mvn skip clean install (minimal build of all modules) - Passing args:  make mcis ARGS="-X"
	$(MCIS) $(POM) $(ARGS)


rebuild-examples: ## rebuild and shade examples - they must be present for integration tests
	$(MCIS) $(POM) -am -pl :sansa-examples-spark_2.12 $(ARGS)
	mvn $(POM) -Pdist,spark-provided,relocate package -pl :sansa-examples-spark_2.12

deploy-snapshot: ## deploy a snapshot of the modules up to ml
	mvn $(POM) -DskipTests deploy -pl :sansa-ml-spark_2.12 -am

shade-examples: ## only shade the examples - use after manual rebuilt of specific modules
	mvn $(POM) -Pdist package -pl :sansa-examples-spark_2.12

integration-tests: ## run the integration tests (you must run rebuild-examples first!)
	mvn $(POM) -pl :sansa-integration-tests_2.12 failsafe:integration-test

conformance-ontop: ## run conforance test suite against ontop
	mvn $(POM) -pl :sansa-query-spark_2.12 test -Dsuites='net.sansa_stack.query.spark.compliance.SPARQL11TestSuiteRunnerSparkOntop' 

conformance-sparqlify: ## run conforance test suite against sparqlify
	mvn $(POM) -pl :sansa-query-spark_2.12 test -Dsuites='net.sansa_stack.query.spark.compliance.SPARQL11TestSuiteRunnerSparkSparqlify' 

.ONESHELL:
dist: ## create the standalone jar-with-dependencies of sansa stack with documentation (javadoc/scaladoc)
	$(MS) $(POM) package -Pdist -pl :sansa-stack-spark_2.12 -am $(ARGS)
	file=`find '$(CWD)/sansa-stack/sansa-stack-spark/target' -name '*-jar-with-dependencies.jar'`
	printf '\nCreated package:\n\n%s\n\n' "$$file"

distjar: ## create only the standalone jar-with-dependencies of sansa stack
	$(MCCS) $(POM) package -Pdist -pl :sansa-stack-spark_2.12 -am $(ARGS)
	file=`find '$(CWD)/sansa-stack/sansa-stack-spark/target' -name '*-jar-with-dependencies.jar'`
	printf '\nCreated package:\n\n%s\n\n' "$$file"

#distjar-hadoop: ## create only the standalone jar-with-dependencies of sansa stack including hadoop
#	$(MCCS) package -Pspark-provided,shade -pl :sansa-stack-spark_2.12 -am $(ARGS)
#	file=`find '$(CWD)/sansa-stack/sansa-stack-spark/target' -name '*-jar-with-dependencies.jar'`
#	printf '\nCreated package:\n\n%s\n\n' "$$file"

deb-rebuild: ## rebuild the deb package (minimal build of only required modules)
	$(MCIS) $(POM) -Pdeb -am -pl :sansa-pkg-deb-cli_2.12 $(ARGS)

.ONESHELL:
deb-reinstall: ## reinstall a previously built deb package
	file=`find $(CWD)/sansa-pkg-parent/sansa-pkg-deb-cli/target | grep '\.deb$$'`
	sudo dpkg -i "$$file"


deb-rere: deb-rebuild deb-reinstall ## rebuild and reinstall deb


rpm-rebuild: ## rebuild the rpm package (minimal build of only required modules
	$(MCIS) -Prpm -am -pl :sansa-pkg-rpm-cli_2.12 $(ARGS)

rpm-reinstall: ## reinstall a previously built rpm package
	file=`find $(CWD)/sansa-pkg-parent/sansa-pkg-rpm-cli/target | grep '\.rpm$$'`
	sudo rpm -U "$$file"

rpm-rere: rpm-rebuild rpm-reinstall ## ## rebuild and reinstall rpm

ontop-deps: ## List ontop deps suitable for use with mvn's -pl option
	@# Note: The first line skips xml comments in the same line
	@# A cleaner solution would build and examine the effective poms
	@grep --include 'pom.xml' -hoPR '.*<artifactId>ontop.*' | grep -v '<!--' | \
	grep -oP '(?<=<artifactId>)ontop.*(?=</artifactId>)' | \
	sort -u | xargs | sed 's/ /,/g'


