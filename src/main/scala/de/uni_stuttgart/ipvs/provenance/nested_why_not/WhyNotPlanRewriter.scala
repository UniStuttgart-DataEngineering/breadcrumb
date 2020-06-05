package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.transformations.{AggregateRewrite, FilterRewrite, GenerateRewrite, ProjectRewrite, RelationRewrite}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Generate, LeafNode, LocalRelation, LogicalPlan, Project, ReturnAnswer, Subquery}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, Expression, Literal, MonotonicallyIncreasingID, NamedExpression}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import javax.management.relation.Relation


object WhyNotPlanRewriter {

  private var oid = 0

  def getUniqueOperatorIdentifier(): Int = {
    oid += 1
    oid
  }

  def rewrite(plan: LogicalPlan, whyNotQuestion: SchemaSubsetTree): Rewrite = {
    plan match {
      case ra: ReturnAnswer =>
      {
        rewrite(ra.child, whyNotQuestion)
      }
      case f: Filter =>
      {
        FilterRewrite(f, whyNotQuestion, getUniqueOperatorIdentifier()).rewrite
      }
      case p: Project =>
      {
        ProjectRewrite(p, whyNotQuestion, getUniqueOperatorIdentifier()).rewrite
      }
      case l: LeafNode =>
      {
        RelationRewrite(l, whyNotQuestion, getUniqueOperatorIdentifier()).rewrite
      }
      case g: Generate =>
      {
        GenerateRewrite(g, whyNotQuestion, getUniqueOperatorIdentifier()).rewrite
      }
      case a: Aggregate =>
      {
        AggregateRewrite(a, whyNotQuestion, getUniqueOperatorIdentifier()).rewrite
      }
    }
    //case plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan => {
    //plan.map(plan => annotateAllChildren)
    //}
  }

}
