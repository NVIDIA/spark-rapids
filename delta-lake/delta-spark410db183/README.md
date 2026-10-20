# Databricks 18.3 Delta reader layer

This module compiles the DBR 18.3 reader dependency subset. Native deletion-vector decoding and
bitmap utilities are shared with DBR 17.3; the DBR 18.3 wrappers keep low-shuffle discovery disabled.

Build the actual reader module with Java 17 after its SQL-plugin dependencies are available:

```bash
mvn -B -f scala2.13/delta-lake/delta-spark410db183/pom.xml \
  -Ddatabricks -Dbuildver=410db183 -DskipTests -Dmaven.test.skip=true \
  -Dmaven.scaladoc.skip clean package
```

The classified `*-spark410db183.jar` contains the compiled reader classes. The root release
profile continues to build the Delta stub. This module does not register a Delta provider or
enable Delta integration tests; transaction and write support remain separate implementation work.
