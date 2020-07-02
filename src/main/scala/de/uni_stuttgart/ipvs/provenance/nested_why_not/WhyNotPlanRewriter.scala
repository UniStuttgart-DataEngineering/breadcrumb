package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.transformations.{AggregateRewrite, FilterRewrite, GenerateRewrite, JoinRewrite, ProjectRewrite, RelationRewrite, TransformationRewrite, UnionRewrite}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Generate, Join, LeafNode, LogicalPlan, Project, ReturnAnswer, Union}


object WhyNotPlanRewriter {

  private var oid = 0

  def getUniqueOperatorIdentifier(): Int = {
    oid += 1
    oid
  }

  def buildRewriteTree(plan: LogicalPlan): TransformationRewrite = {
    plan match {
      case ra: ReturnAnswer => {
        buildRewriteTree(ra.child)
      }
      case f: Filter => {
        FilterRewrite(f, getUniqueOperatorIdentifier())
      }
      case p: Project => {
        ProjectRewrite(p, getUniqueOperatorIdentifier())
      }
      case l: LeafNode => {
        RelationRewrite(l, getUniqueOperatorIdentifier())
      }
      case g: Generate => {
        GenerateRewrite(g, getUniqueOperatorIdentifier())
      }
      case a: Aggregate => {
        AggregateRewrite(a, getUniqueOperatorIdentifier())
      }
      case j: Join => {
        JoinRewrite(j, getUniqueOperatorIdentifier())
      }
      case u: Union => {
        UnionRewrite(u, getUniqueOperatorIdentifier())
      }
      case default => {
        throw new MatchError("Unsopported operator:" + default.nodeName)
      }
    }
  }

}
