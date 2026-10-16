# Databricks 18.3 Delta Lake implementation

This module contains the DBR 18.3 reader, transaction, catalog, and write implementations.
The default DBR 18.3 build continues to use `delta-stub` until build activation is enabled.

After installing the DBR 18.3 root reactor dependencies, build this module explicitly
with Java 17:

```bash
mvn -B -f scala2.13/delta-lake/delta-spark410db183/pom.xml \
  -Ddatabricks -Dbuildver=410db183 -DskipTests -Dmaven.scaladoc.skip clean verify
```

Runtime integration coverage is tracked in NVIDIA/cudf-spark#16282.
