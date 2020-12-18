---
layout: page
title: Testing
nav_order: 1
---
# RAPIDS Accelerator for Testing against the upcoming version of Apache Spark

While writing unit-tests, we can run into situations where we depend on classes that are not
 available in the default version of Spark (currently 3.0.0). In such a scenario we put
  those tests in this module. As of writing of this document we put tests that are strongly tied to 
  classes in Spark-3.1.0-SNAPSHOT here. 
  
  These tests can be executed by giving the profile `spark310tests` 
  
  Example: 
  `mvn -Pspark310tests -wildcardSuites=<testname wildcard>`

For default unit-tests please refer to the unit-tests [README](../tests/README.md)
For integration-tests please refer to the integration-tests [README](../integration_tests/README.md)
