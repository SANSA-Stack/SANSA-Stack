
Debian packaging of the sansa-spark-cli module.
Requires activation of the `db` profile in order to be built:

```bash
mvn -Pdeb clean package
sudo dpkg -i target/sansa-spark-cli_$VERSION$_all.deb
```


