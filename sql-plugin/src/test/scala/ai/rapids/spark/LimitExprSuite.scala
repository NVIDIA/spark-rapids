package ai.rapids.spark

class LimitExprSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("limit more than rows", intCsvDf) {
      frame => frame.limit(10).repartition(2)
  }

  testSparkResultsAreEqual("limit less than rows", intCsvDf) {
    frame => frame.limit(1).repartition(2)
  }
}
