package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{AnnotationExtension, Constants, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.StructField


object LocalRelationRewrite {
  def apply(localRelation: LocalRelation, whyNotQuestion:SchemaMatch, oid: Int)  = new LocalRelationRewrite(localRelation, whyNotQuestion, oid)
}

class LocalRelationRewrite(localRelation: LocalRelation, whyNotQuestion:SchemaMatch, oid: Int) extends TransformationRewrite(localRelation, whyNotQuestion, oid){

  def getFieldNameWithOid(fieldName: String): String = {
    fieldName + "__OID_" + oid.toString
  }

  override def rewrite: Rewrite = {
    //we assume this is an initial operator without any children
   //StructField(getFieldNameWithOid(Constants.String), )

    //localRelation.output
    Rewrite(localRelation, new ProvenanceContext())
  }





}
