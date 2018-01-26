package main

/**
  * Created by b79073 on 18/07/2017.
  */

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import tool.Setup


/**
  * Run MainClass with :
  *
  * export HADOOP_CMD=/usr/bin/hadoop
  * export HADOOP_CONF_DIR=/etc/hadoop/conf
  * export JAVA_HOME=<>
  * export SPARK_HOME=<>
  *
  *
  * ${SPARK_HOME}/bin/spark-submit \
  * --class main.MainClass \
  * --conf spark.driver.userClassPathFirst=true  \
  * --master yarn \
  * fpspp-spark-1.0-SNAPSHOT-UBER.jar <package.class>
  * Note : Ajouter les options dans "Run... -> Edit Configuration..."
  * -Xms128m -Xmx512m -XX:MaxPermSize=300m -ea
  */
object MainClass {

  val spark: SparkSession = Setup.getSparkSession("SparkLDA")
  val sc: SparkContext = spark.sparkContext
  val PROP: PropertiesConfiguration = new PropertiesConfiguration()
  val LOGGER: Logger = LogManager.getRootLogger


  def main(args: Array[String]) {

    val in = getClass.getResourceAsStream("/data.properties")
    PROP.load(in)



    //Launch Script
    implicit def newify[T](className: String) = Class.forName(className).newInstance.asInstanceOf[T]
    args.foreach{ nom_class =>
      val runJob:RunJob = nom_class;
      runJob.run()
    }
  }
}
