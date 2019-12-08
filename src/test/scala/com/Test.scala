package com

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class UtilitiesTestSpec extends FunSuite with BeforeAndAfterEach {

  var session: SparkSession = _

  val path = "c:\\IdeaProjects\\DataAnalyzer\\in\\Clickstream\\"

  override def beforeEach(): Unit = {
    session = SparkSession.builder().appName("Data Analyzer").master("local[1]").getOrCreate()
  }

  test("creating data frame from text file") {
    //import sparkSession.implicits._
    //val peopleDF = ReadAndWrite.readFile(sparkSession, "src/test/resources/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()
    //peopleDF.printSchema()
   // assert(peopleDF.count() == 3)
  }
}