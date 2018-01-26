package tool

import java.io.{FileInputStream, IOException}
import java.nio.file.Files
import java.util.regex.Pattern

import main.MainClass
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object Setup {
  val CLIENT_MODE="client"


  /**
    * Test if current OS is windows or not
    */
  def isWindow(): Boolean = {
    return System
      .getProperty("os.name")
      .startsWith("Windows")
  }

  /**
    * Get Spark Context :
    *   - with local master if windows
    */
  def getSparkSession(nameApp : String): SparkSession = {

    val conf = new SparkConf().setAppName(nameApp).set("spark.driver.allowMultipleContexts","true")
      .set("es.index.auto.create","true")
      .set("es.nodes","") // Conf
      .set("es.port","")

    // Initialize master
    if (Setup.isWindow()){
      conf.setMaster("local");
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
    }


    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()



    // Return Spark Context
    return spark;
  }



  def getSparkContext(nameApp : String): SparkContext = {
    val spark = getSparkSession(nameApp)
    return spark.sparkContext
  }



}
