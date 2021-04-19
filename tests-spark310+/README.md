# RAPIDS Accelerator for Testing against the upcoming version of Apache Spark

While writing unit-tests, we can run into situations where we depend on classes that are only 
available in specific versions of Spark. In such a scenario we put those tests in this module. 

As of writing of this document this module contains tests that are strongly tied to classes in 
Spark-3.1.1+.
  
These tests can be executed by choosing profile `spark311tests` like so, 
  
`mvn -Pspark311tests -wildcardSuites=<testname wildcard>`

For a more comprehensive overview of tests in Rapids Accelerator please refer to the following 

- For unit-tests please refer to the unit-tests [README](../tests/README.md)
- For integration-tests please refer to the integration-tests [README](../integration_tests/README.md)
