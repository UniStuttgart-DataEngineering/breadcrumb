package de.uni_stuttgart.ipvs.provenance

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SharedSparkTestInstance {
  lazy val spark =
    SparkSession.builder
      .appName("SharedSparkTestInstance")
      .master("local[2]")
      .getOrCreate()
  Logger.getLogger("org").setLevel(Level.WARN)
}



trait SharedSparkTestInstance
{
  lazy val spark = SharedSparkTestInstance.spark

  def checkSchemaContainment(containingDataFrame: DataFrame, containedDataFrame: DataFrame ): Boolean = {
    val containedColumnNames = containedDataFrame.schema.map(field => field.name).toSet
    val containingColumnNames = containedDataFrame.schema.map(field => field.name).toSet
    containedColumnNames.subsetOf(containingColumnNames)
  }
}


