package analytics

import java.time.LocalDate

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Utils

import scala.util.{Failure, Success, Try}

trait Analytics {

  @transient val logger = Logger.getLogger( getClass )

  /**
   *
   * @param path  - path clients directory
   * @param session - spark session
   * @param dataFrame - current data frame
   * @param clientName - client name
   * @param daysBack - to date
   * @return
   */
  def analyzeNumOfActivities( path: String, session: SparkSession, dataFrame: DataFrame, clientName: String, from: String, to: String, daysBack: Int  ) = {

    val resultsPath = s"$path/$clientName/results/${LocalDate.now.toString}/activities"
    Try {
      val activitiesStats = session.sql(s"SELECT count(DISTINCT(activity)),user_id, account_id, activity FROM analytics where to_date < $to and to_date > $from group by user_id, account_id, module, activity")
      activitiesStats.show()
      logger.info( activitiesStats.explain(true))


      Utils.createDirectory( resultsPath )
      persist(s"$resultsPath/$daysBack", activitiesStats)

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
  def analyzeNumOfUniqueUsers( path: String, session: SparkSession, dataFrame: DataFrame, clientName: String, from: String, to: String, daysBack: Int  ) = {

    val resultsPath = s"$path/$clientName/results/${LocalDate.now.toString}/uniques"
    Try {
      val uniquesDF = session.sql(s"SELECT COUNT(DISTINCT(user_id)),user_id, account_id FROM analytics where to_date < $to and to_date > $from group by account_id, user_id")
      uniquesDF.show
      logger.info( uniquesDF.explain())

      Utils.createDirectory( resultsPath )
      persist(s"$resultsPath/$daysBack", uniquesDF )

    } match {
      case Success(_) => logger.info(s"Saved successfully $resultsPath")
      case Failure(ex) => {
        logger.error(s"Failed to persist - ${resultsPath}", ex)
        false
      }
      //userIDStats.sa
    }
  }

  def analyzeNumOfModules( path: String, session: SparkSession, dataFrame: DataFrame, clientName: String, from: String, to: String, daysBack: Int   ) = {

    val resultsPath = s"$path/$clientName/results/${LocalDate.now.toString}/modules"
    Try{
      val modulesDF = session.sql(s"SELECT count(DISTINCT(module)),user_id, account_id FROM analytics where to_date < $to and to_date > $from group by user_id, account_id")
      modulesDF.show
      logger.info( modulesDF.explain() )


      Utils.createDirectory( resultsPath )
      persist( s"$resultsPath/$daysBack", modulesDF )

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
