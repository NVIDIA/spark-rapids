# Databricks 18.3 Delta Lake implementation

The default DBR 18.3 build continues to use `delta-stub`. This module currently compiles
the reader implementation and its dependencies without registering a Delta provider.

After installing the DBR 18.3 root reactor dependencies, build the reader module explicitly
with Java 17:

```bash
mvn -B -f scala2.13/delta-lake/delta-spark410db183/pom.xml \
  -Ddatabricks -Dbuildver=410db183 -DskipTests -Dmaven.scaladoc.skip clean verify
```

The module's source selection will expand when the transaction and write layer is added.
Runtime integration coverage is tracked in NVIDIA/cudf-spark#16282.
