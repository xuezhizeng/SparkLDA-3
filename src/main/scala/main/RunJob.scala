package main

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import tool.Setup

/**
  * Created by gportier on 16/11/2017.
  */
abstract class RunJob{
  private val spark: SparkSession = MainClass.spark
  private val sc: SparkContext = MainClass.sc
  private val PROP : PropertiesConfiguration = MainClass.PROP
  private val LOGGER : Logger = MainClass.LOGGER

  def run();
}