package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}

object AnnotateCandidates {
  def apply(basePlan: LogicalPlan, pq: Expression): LogicalPlan = {
    val candidateAttr = Alias(pq, Literal("candidate").toString())()
    val projExprs = basePlan.output :+ candidateAttr

    Project(
      projExprs,
      basePlan
    )
  }

}
