CWD = $(shell pwd)

MCIS = mvn -DskipTests=true -Dmaven.javadoc.skip=true -Dskip

.PHONY: all
all:
	echo "Run `make help` to list available goals"


deb-build:
	$(MCIS) -Pdeb -am -pl :sansa-pkg-deb-cli_2.12

.ONESHELL:
deb-reinstall:
	p1=`find $(CWD)/sansa-pkg-parent/sansa-pkg-deb-cli/target | grep '\.deb$$'`
	sudo dpkg -i "$$p1"

rpm-build:
	$(MCIS) -Prpm -am -pl :sansa-pkg-rpm-cli_2.12 clean install

rpm-reinstall:
	p1=`find $(CWD)/sansa-pkg-parent/sansa-pkg-rpm-cli/target | grep '\.deb$$'`
	# sudo dpkg -i "$$p1"


