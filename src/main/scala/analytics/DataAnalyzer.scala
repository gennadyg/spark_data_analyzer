package analytics

import java.net.URI
import java.time.LocalDate

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import utils.Utils

import scala.util.{Failure, Success, Try}


/**
You need to build a system that will do a daily batch processing (according to each customer’s timezone),
that will allow data analysts to query this
usage statistics, and draw some insights. The input data is clickstream information that is kept in S3, in files that contain a bunch of JSON
documents in them. Each document represents 1 event. Each such file represents 1 minute of data.

The statistics we want to compute for each customer are:

 1. What is the number of activities used per user per account per module in the last 1, 3 ,7, 14, 30, 90, 180, 365 days
 2. What is the number of modules used per user per account in the last 1, 3, 7, 14, 30, 90,  180, 365 days
 3. What is the number of unique users per account in the last 1, 3, 7, 14, 30, 90, 180, 365 days

What architecture do you suggest that will support those requirements?

 - EMR/Data Proc Spark cluster that created by scheduler and keep results on S3

How will you schedule this computation?

 - Will use some flow management software: AirFlow, chrontab, etc.

How will you do the actual computation?

  - Will query data based on required time periods and will aggregate timw window, to avoid duplicated data reads

Where will you keep the results?

  - Distributed storage - S3 or some distributed DB, depends on requirements.

ow will you make sure the system can handle up to 1 year of data in a timely manner?
2. How is the Activity and Module Aggregation calculated?
Write a spark program that shows that.
3. How is the number of unique users calculated?

 */
case class DataAnalyzer( session: SparkSession, dataFrameReader: DataFrameReader ) extends Analytics{

  import DataAnalyzer.logger

  val configuration = new Configuration

  def calculateStats( daysBack: Int ) = {

    val to = LocalDate.now
    val from = to.minusDays( daysBack )

    Try{

    }match {
      case Success( value ) => value
      case _ => {
        logger.error(s"Failed to calculate date with  offset - ${daysBack}")
        false
      }
    }


  }
  def createSparkSession = {

    val session = SparkSession.builder().appName("Data Analyzer").master("local[1]").getOrCreate()
    ( session, session.read )
  }

  /**
   *
   * @param dataFrameReader - dataframes reader
   * @param clientRepoPath - path to client repository
   * @param dates
   *
   * @return dataframe - actual data frame with data
   */
  def createDataFrame( dataFrameReader: DataFrameReader, clientRepoPath: String, dates: String ) = {

    dataFrameReader
      .option("multiLine", value = true)
   //  .option("mode", "PERMISSIVE")
      .json(s"$clientRepoPath\\{$dates}\\*")
    //  .withColumn("filename", input_file_name )
  }

  /**
   *
   * @param path - valida path to data repository
   *  @return
   */
  def getClientsList( path : String ): List[String] = {

    FileSystem.get( URI.create( path ), configuration ).globStatus( new Path( path ))
      .filter( fileStatus => fileStatus.isDirectory )
      .map( fileStatus => fileStatus.getPath.getName )
      .toList
  }



  /**
   *
   * @param path
   * @param ranges
   * @return
   */
  def analyze( path: String, ranges: Array[Int] ) = {

    Try{

      getClientsList( s"$path/*").map( client => {
      logger.info( s"Processing client - $client")

      val to = LocalDate.now
      var globalDataFrame: Option[sql.DataFrame] = None

      for( currentRange <- ranges ){
        val currentFrom = LocalDate.now.minusDays( currentRange )
        logger.info(s"Calculating data frame from $currentFrom to $to")

        val clientRepoPath = s"$path/$client/"
        val dates = Utils.generateListOfDates( clientRepoPath, currentFrom, to )
        if( dates.isEmpty == false ){
          val dataFrame = createDataFrame( dataFrameReader, clientRepoPath, dates )
          dataFrame.printSchema()
          dataFrame.show

          dataFrame.createOrReplaceTempView("analytics")
          analyzeNumOfActivities( path, session, dataFrame, client, currentRange  )
          analyzeNumOfUniqueUsers( path, session, dataFrame, client, currentRange )
          analyzeNumOfModules( path, session, dataFrame, client, currentRange )
          //dataFrame.show()

          globalDataFrame match {
            case Some(frame) => {
              globalDataFrame = Some( frame.union( dataFrame ))
              globalDataFrame.get.show()
            }
            case None => globalDataFrame = Some(dataFrame)
          }
        }else{
          logger.warn(s"No dates were found for $client during[$currentFrom -  $to]")
        }
      }
      })
      session.close()

    } match {
      case Success( value ) => value
      case Failure( ex ) => {
        logger.error(s"Failed process path - ${path}", ex )
        false
      }
    }


  }

}

object DataAnalyzer {

  @transient val logger = Logger.getLogger( getClass )

  def main(args: Array[String]) {

    val ranges = Array( 1, 3, 7, 14, 30, 90, 180, 365 )
    val path = "in/Clickstream"

  // val date = LocalDate.parse("2019-12-06")
    val session = SparkSession.builder().appName("Data Analyzer").master("local[1]").getOrCreate()
      DataAnalyzer( session, session.read ).analyze( path, ranges )
//    dataAnalyzer.analyze( "in/Clickstream", ranges )

    //  logger.info( s"Processing client - $client")

    /*dataFrame.printSchema()
    dataFrame.show()
    dataFrame.createOrReplaceTempView("people")*/


  }
}
