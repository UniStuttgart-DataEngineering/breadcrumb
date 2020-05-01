package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Rewrite
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class TransformationRewrite(val plan: LogicalPlan, val whyNotQuestion: SchemaMatch, val oid: Int) {

  def unrestructure(child: Option[LogicalPlan] = None): SchemaMatch ={
    //child is only needed if operator has more than one input
    whyNotQuestion
  }

  def rewrite():Rewrite = null

}
