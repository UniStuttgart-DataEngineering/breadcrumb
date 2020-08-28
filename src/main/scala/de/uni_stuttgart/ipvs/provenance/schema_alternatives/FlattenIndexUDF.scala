package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1

class FlattenIndexUDF extends UDF1[Int, Array[Int]] {

  override def call(size: Int): Array[Int] = {
    (1 to size).toArray
  }


}
