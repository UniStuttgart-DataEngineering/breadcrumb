package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.{UDF2}

import scala.collection.mutable

class MSRComputationAlternativeUDF extends UDF2[Row, Seq[Int], mutable.WrappedArray[String]] {
  //class MSRComputationUDF extends UDF1[Row, Boolean] {

  val oid_pattern = """\d+""".r


  def formatOid(oid:Int): String = {
    f"${oid}%04d"
  }

  def getOperatorIdString(fieldName: String): String = {
    (oid_pattern findFirstIn fieldName).get
  }

  //override def call(row: Row): Tuple1[mutable.WrappedArray[String]] = {
  override def call(row: Row, alternativeOps: Seq[Int]): mutable.WrappedArray[String] = {

    var pickyOperators = mutable.Set.empty[String]
    pickyOperators ++= alternativeOps.map(formatOid(_))

    val survivorColumns = row.schema.fields.map(field => field.name).filter(name => Constants.isSurvivedField(name))


    for(col <- survivorColumns) {
      if (row.getAs[Boolean](col) == false){
        pickyOperators += getOperatorIdString(col)
      }
    }
    val res = mutable.WrappedArray.empty[String] ++ pickyOperators
    res.sorted

  }

}
