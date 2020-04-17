package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}


object AnnotationEncoder {
  def apply() = new AnnotationEncoder()
}

class AnnotationEncoder {

  /*
  def annotationStruct(fieldNames:Seq[String]): StructType = {
    annotationStruct(StructType(fieldNames.map { f => StructField(f, StringType, true) }))
  }
  */

  def rowAnnotationExpression(
                               annotation: String = PROVENANCE_ID_STRUCT
                             ): Expression =
    UnresolvedExtractValue(
      UnresolvedAttribute(annotation),
      Literal(PROVENANCE_ID_FIELD)
    )

  def annotationStruct(): StructType = {
    StructType(Seq(
      StructField(PROVENANCE_ID_FIELD, LongType, false),
      StructField(PROVENANCE_ID_FIELD + "_CONST", LongType, false)
    ))
  }

}
