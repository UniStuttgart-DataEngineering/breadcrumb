package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.logical.Join

object JoinRewrite {
  def apply(join: Join, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new JoinRewrite(join, whyNotQuestion, oid)
}

class JoinRewrite (val join: Join, override val whyNotQuestion: SchemaSubsetTree, override val oid: Int) extends BinaryTransformationRewrite(join, whyNotQuestion, oid) {
  override def unrestructureLeft(): SchemaSubsetTree = {
    //TODO implement
    whyNotQuestion
  }

  override def unrestructureRight(): SchemaSubsetTree = {
    //TODO implement
    whyNotQuestion
  }

  override def rewrite(): Rewrite = {

    //unrestructure whynotquestion
    //rewrite left child
    //rewrite right child
    //merge provenance structure
    //rewrite plan

    val leftWhyNotQuestion = unrestructureLeft()
    val rightWhyNotQuestion = unrestructureRight()
    val leftRewrite = WhyNotPlanRewriter.rewrite(join.left, leftWhyNotQuestion)
    val rightRewrite = WhyNotPlanRewriter.rewrite(join.right, rightWhyNotQuestion)
    val provenanceContext = ProvenanceContext.mergeContext(leftRewrite.provenanceContext, rightRewrite.provenanceContext)

    val rewrittenJoin = Join(leftRewrite.plan, rightRewrite.plan, FullOuter, join.condition)




    Rewrite(rewrittenJoin, provenanceContext)





  }
}
