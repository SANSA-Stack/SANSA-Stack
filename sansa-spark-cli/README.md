
## Common commands (to be run from the root of the repo)

```bash
# Rebuild the cli module
mvn -pl sansa-spark-cli clean install

# Rebuild the deb package
mvn -Pdeb -pl sansa-debian-spark-cli clean install

# Locate the deb package in the target folder and install it using "sudo dpkg -i your.deb"
./reinstall-debs.sh
```
