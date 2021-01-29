## sansa-metadata

The module only contains a simple property file resource with placeholders for version and build time.
An instance of the file is created on every build of this module and put on the classpath with the placeholders filled out.

* [template file](src/main/resources-filtered/sansa.properties): `src/main/resources-filtered/sansa.properties`
* [instance file](target/classes/sansa.properties) - only generated on build: `target/classes/sansa.properties`

Hence this information is bundled with the jar which allows for reading it out during runtime and presenting it to
developers and users.

Examples include command line interfaces or web services built on sansa.

This reason this is a standalone module is to make devs / users aware that this feature exists.

