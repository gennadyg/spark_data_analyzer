package analytics

import java.time.LocalDate

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utils

import scala.util.{Failure, Success, Try}

trait Analytics {

  @transient val logger = Logger.getLogger( getClass )


  def analyzeNumOfActivities( path: String, session: SparkSession, dataFrame: DataFrame, clientName: String, currentRange: Int  ) = {

    val resultsPath = s"$path/$clientName/results/${LocalDate.now.toString}/activities"
    Try {
      val activitiesStats = session.sql("SELECT count(activity),user_id, account_id, activity FROM analytics group by user_id, account_id, module, activity")
      activitiesStats.show()
      Utils.createDirectory( resultsPath )
      persist(s"$resultsPath/$currentRange", activitiesStats)

    } match {
      case Success(_) => logger.info(s"Saved successfully $resultsPath")
      case Failure(ex) => {
        logger.error(s"Failed to persist - ${resultsPath}", ex)
        false
      }
    }
  }

  /**
   *
   *
   * @param dataFrame - data frame
   */
  def analyzeNumOfUniqueUsers( path: String, session: SparkSession, dataFrame: DataFrame, clientName: String, currentRange: Int ) = {

    val resultsPath = s"$path/$clientName/results/${LocalDate.now.toString}/uniques"
    Try {
      val uniquesDF = session.sql("SELECT count(user_id),user_id, account_id FROM analytics group by account_id, user_id")
      uniquesDF.show

      Utils.createDirectory( resultsPath )
      persist(s"$resultsPath/$currentRange", uniquesDF )

    } match {
      case Success(_) => logger.info(s"Saved successfully $resultsPath")
      case Failure(ex) => {
        logger.error(s"Failed to persist - ${resultsPath}", ex)
        false
      }
      //userIDStats.sa
    }
  }

  def analyzeNumOfModules( path: String, session: SparkSession, dataFrame: DataFrame, clientName: String, currentRange: Int  ) = {

    val resultsPath = s"$path/$clientName/results/${LocalDate.now.toString}/modules"
    Try{
      val modulesDF = session.sql("SELECT count(module),user_id, account_id FROM analytics group by user_id, account_id")
      modulesDF.show

      Utils.createDirectory( resultsPath )
      persist( s"$resultsPath/$currentRange", modulesDF )

    } match {
      case Success(_) => logger.info(s"Saved successfully $resultsPath")
      case Failure(ex) => {
        logger.error(s"Failed to persist - ${resultsPath}", ex )
        false
      }
    }

   //userIDStats.sa
  }

    /**
     *
      * @param path - path to client data
     *  @param dataFrame - stats data
     */
   def persist( path: String, dataFrame: DataFrame ) = {

      if( dataFrame.count() > 0  ){
        dataFrame.coalesce(1).write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save( path )
      }
   }

}
