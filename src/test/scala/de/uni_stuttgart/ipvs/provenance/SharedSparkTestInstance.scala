package de.uni_stuttgart.ipvs.provenance

import org.apache.spark.sql.SparkSession

object SharedSparkTestInstance {
  lazy val spark =
    SparkSession.builder
      .appName("SharedSparkTestInstance")
      .master("local[2]")
      .getOrCreate()
}

trait SharedSparkTestInstance
{
  lazy val spark = SharedSparkTestInstance.spark
}


