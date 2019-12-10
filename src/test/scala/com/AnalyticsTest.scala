package com

import analytics.DataAnalyzer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, WordSpec}

class AnalyticsTest extends WordSpec with Matchers with SparkContextSetup {
  "Daily test" should {
    "calculate the right thing" in withSparkContext { (session, dataAnalyzer) =>
      val ranges = Array( 1 )
      val dailyDF = dataAnalyzer.createDailyDataFrame( "in\\Clickstream\\ibm\\", "2019-12-01", "ibm" )
      dailyDF.show()
      dailyDF.createOrReplaceTempView("analytics")
      val activitiesStats = session.sql(s"SELECT count(distinct(activity)) as numOfAct,user_id, account_id, activity FROM analytics where date >= '2019-12-01' group by user_id, account_id, module, activity").first
      val activities = activitiesStats.get(0)
      assert( activities == 1)
      val uniqueUsers = session.sql(s"SELECT COUNT(DISTINCT(user_id)),user_id, account_id FROM analytics where date >= '2019-12-01' group by account_id, user_id").first()
      val uniques = uniqueUsers.get(0)
      assert( uniques == 1)

      val modules = session.sql(s"SELECT count(DISTINCT(module)),user_id, account_id FROM analytics where date >= '2019-12-01' group by user_id, account_id").first
      val modulesStats = modules.get(0)
      assert( modulesStats == 1)
      //total shouldBe 1000
    }
  }
}

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkSession, DataAnalyzer) => Any) {
    val session = SparkSession.builder().appName("Data Analyzer").master("local[1]").getOrCreate()
    val dataAnalyzer = DataAnalyzer(session, session.read)
    try {
      testMethod(session, dataAnalyzer)
    }
    finally session.close()
  }
}
