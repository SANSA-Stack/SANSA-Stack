CWD = $(shell pwd)

# Maven Clean Install Skip ; skip tests, javadoc, scaladoc, etc
MS = mvn -DskipTests=true -Dmaven.javadoc.skip=true -Dskip
MCIS = $(MS) clean install

# Source: https://stackoverflow.com/questions/4219255/how-do-you-get-the-list-of-targets-in-a-makefile
.PHONY: help
help:  ## Show these help instructions
	@sed -rn 's/^([a-zA-Z_-]+):.*?## (.*)$$/"\1" "\2"/p' < $(MAKEFILE_LIST) | xargs printf "make %-20s# %s\n"

mcis: ## mvn skip clean install (minimal build of all modules) - Passing args:  make mcis ARGS="-X"
	$(MCIS) $(ARGS)


rebuild-examples: ## rebuild and shade examples - they must be present for integration tests
	$(MCIS) -am -pl :sansa-examples-spark_2.12 $(ARGS)
	mvn -Pdist package -pl :sansa-examples-spark_2.12

shade-examples: ## only shade the examples - use after manual rebuilt of specific modules
	mvn -Pdist package -pl :sansa-examples-spark_2.12

integration-tests: ## run the integration tests
	mvn -pl :sansa-integration-tests_2.12 failsafe:integration-test

deb-rebuild: ## rebuild the deb package (minimal build of only required modules)
	$(MCIS) -Pdeb -am -pl :sansa-pkg-deb-cli_2.12 $(ARGS)

.ONESHELL:
deb-reinstall: ## reinstall a previously built deb package
	file=`find $(CWD)/sansa-pkg-parent/sansa-pkg-deb-cli/target | grep '\.deb$$'`
	sudo dpkg -i "$$file"

rpm-rebuild: ## rebuild the rpm package (minimal build of only required modules
	$(MCIS) -Prpm -am -pl :sansa-pkg-rpm-cli_2.12 $(ARGS)

rpm-reinstall: ## reinstall a previously built rpm package
	file=`find $(CWD)/sansa-pkg-parent/sansa-pkg-rpm-cli/target | grep '\.rpm$$'`
	sudo rpm -U "$$file"


