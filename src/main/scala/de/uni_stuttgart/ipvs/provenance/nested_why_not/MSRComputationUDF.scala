package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StructField

import scala.util.matching.Regex
import scala.collection.mutable

class MSRComputationUDF extends UDF1[Row, mutable.WrappedArray[String]] {
//class MSRComputationUDF extends UDF1[Row, Boolean] {

  val oid_pattern = """\d+""".r

  def getOperatorIdString(fieldName: String): String = {
    (oid_pattern findFirstIn fieldName).get
  }

  //override def call(row: Row): Tuple1[mutable.WrappedArray[String]] = {
  override def call(row: Row): mutable.WrappedArray[String] = {

    var pickyOperators = mutable.WrappedArray.empty[String]

    val survivorColumns = row.schema.fields.map(field => field.name)


    for(col <- survivorColumns) {
      if (!row.getAs[Boolean](col)){
        pickyOperators = pickyOperators :+ getOperatorIdString(col)
      }
    }
    pickyOperators
    //true

  }
}
