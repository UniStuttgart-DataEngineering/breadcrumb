package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Rewrite
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class TransformationRewrite(val plan: LogicalPlan, val whyNotQuestion: SchemaSubsetTree, val oid: Int) {

  def unrestructure(child: Option[LogicalPlan] = None): SchemaSubsetTree ={
    //child is only needed if operator has more than one input
    whyNotQuestion
  }

  def rewrite():Rewrite

}
