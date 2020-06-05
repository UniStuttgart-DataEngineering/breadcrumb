package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Rewrite
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

trait TransformationRewrite {

  def plan: LogicalPlan
  def whyNotQuestion: SchemaSubsetTree
  def oid: Int

  def rewrite():Rewrite

}
