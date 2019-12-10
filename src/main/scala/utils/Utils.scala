package utils

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

object Utils{


  @transient val logger = Logger.getLogger( getClass )
  val DATE_FORMAT = "YYYY-MM-d"
  val dateFormat = DateTimeFormatter.ofPattern(DATE_FORMAT)

  val configuration = new Configuration

  /**
   * Creates directory
   *
   * @param dirName - S3 path
   * @return
   */
  def createDirectory( dirName : String ) = {
    Try {
      val path = new Path( dirName )
      val fileSystem = path.getFileSystem( configuration )
      logger.info(s"Creating directory - $dirName")
      fileSystem.mkdirs( path )
    } match {

      case Success( value ) => true
      case Failure(exception) => {

        logger.error("Failed to create directory", exception )
        false
      }
    }
  }

  /**
   *
   * @param path
   * @return
   */
  def isDirectory( path : String ): Boolean = {

    Try {

      FileSystem.get(URI.create( path ), new Configuration() ).isDirectory(new Path(path))

    } match {
      case Success( value ) => value
      case _ => {
        logger.info(s"Directory desn't exists - $path")
        false
      }
    }
  }
  /**
   * Generates string of comma separated dates
   *
   * @param path - path to data repository
   *
   * @return string of comma separated dates
   */
  def generateSetOfDates( path: String, from: LocalDate, to: LocalDate ): Set[String] = {
    Try {
      val list = FileSystem.get(URI.create(path), configuration).globStatus(new Path(s"$path/{2019,2020}*"), RegexPathFilter(from, to))

      list.map(validFile => {
        logger.info(s"Found valid folder/date ${validFile.getPath.getName}") ///${validFile.getPath.toUri.getRawPath}
        validFile.getPath.getName
      }).toSet[String]

    } match {
      case Success(value) => value
      case _ => {
        logger.error(s"Failed to generate list of date for path ${path}")
        Set.empty
      }
    }

  }
  /**
   * Generates string of comma separated dates
   *
   * @param path - path to data repository
   *
   * @return string of comma separated dates
   */
  def generateListOfDates( path: String, from: LocalDate, to: LocalDate ): String = {
    Try {
      val list = FileSystem.get(URI.create(path), configuration).globStatus(new Path(s"$path/{2019,2020}*"), RegexPathFilter(from, to))
      list.map(validFile => {
        logger.info(s"Found valid folder/date ${validFile.getPath.getName}") ///${validFile.getPath.toUri.getRawPath}
        validFile.getPath.getName
      }).toList.mkString(",")

    } match {
      case Success(value) => value
      case _ => {
        logger.error(s"Failed to generate list of date for path ${path}")
        ""
      }
    }

  }


  def getDatesRange( daysBackSet: Array[Int] ) = {

    val offsetsMap = scala.collection.mutable.Map( 0 -> LocalDate.now )
    Try{

      for( currentDay <-  daysBackSet ){
        offsetsMap( currentDay ) = LocalDate.now.minusDays( currentDay )
      }
      Some( offsetsMap )

    } match {
      case Success( value ) => value
      case _ => {
        logger.error(s"Failed to generate dates range - ${daysBackSet.toString}")
        None
      }
    }
  }
}
/**
 *
 * @param from
 * @param to
 */
class RegexPathFilter( from: LocalDate, to: LocalDate ) extends PathFilter{

  @transient val logger = Logger.getLogger( getClass )

  override def accept( path: Path ): Boolean = {
    Try{
        val fileName = path.getName
        logger.info(s"Checking date - ${fileName}" )
        val date = LocalDate.parse( fileName )
        date.equals(from) || date.equals(to) || ( date.isAfter( from ) && date.isBefore( to ))

    } match {
      case Success( value ) => value
      case Failure(ex) => {
        logger.error(s"Failed to process file - ${path.getName}", ex )
        false
      }
    }

  }
}
object RegexPathFilter{
  def apply(from: LocalDate, to: LocalDate): RegexPathFilter = new RegexPathFilter( from, to)
}
