package ai.rapids.spark

import org.apache.spark.SparkConf

class LimitExprSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual("limit more than rows", intCsvDf) {
      frame => frame.limit(10).repartition(2)
  }

  testSparkResultsAreEqual("limit less than rows", intCsvDf) {
    frame => frame.limit(1).repartition(2)
  }

  testSparkResultsAreEqual("limit batchSize", intCsvDf,
    conf = makeBatched(1)) {
    frame => frame.limit(1).repartition(2)
  }

}
