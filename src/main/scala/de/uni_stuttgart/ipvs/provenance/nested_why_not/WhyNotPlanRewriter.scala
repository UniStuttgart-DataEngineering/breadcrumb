package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stutde.uni_stuttgart.ipvs.provenance.transformations.FilterRewrite
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, ReturnAnswer, Subquery}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, Expression, Literal, MonotonicallyIncreasingID, NamedExpression}
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._


object WhyNotPlanRewriter {

  def annotationEncoding = AnnotationEncoder

  protected def applyOnChildren(plan: LogicalPlan, pq: Expression): LogicalPlan = {
    plan.mapChildren(apply(_,pq))
  }

  def apply(plan: LogicalPlan, pq: Expression): LogicalPlan = {
    val annotatedPlan = plan match {
      case _:ReturnAnswer =>
      {
        /*
          A node automatically inserted at the top of query plans to allow
          pattern-matching rules to insert top-only operators.
        */
        applyOnChildren(plan,pq)
      }
      case _:Subquery =>
      {
        /*
          A node automatically inserted at the top of subquery plans to
          allow for subquery-specific optimizatiosn.
        */
        applyOnChildren(plan,pq)
      }
      case filter: Filter => {
        val rewrittenChild = apply(filter.child,pq)
//        val modifiedFilter = Filter(filter.condition, rewrittenChild)

        // annotating candidates
        val candidatesOnChild = AnnotateCandidates(rewrittenChild,pq)

        // rewriting plan for filter
        val modifiedFilter = new FilterRewrite() apply(filter.condition, candidatesOnChild)
        val projection = modifiedFilter.output :+ buildAnnotation(modifiedFilter)

        Project(
          projection,
          modifiedFilter
        )
      }
      case x => {
        applyOnChildren(plan,pq)
      }

    }
    annotatedPlan
  }

  protected def buildAnnotation(
                                 plan: LogicalPlan,
                                 annotationAttr: String = PROVENANCE_ID_STRUCT
                               ): NamedExpression = {
    val columns = plan.output.map(_.name)

    Alias(
      CreateNamedStruct(Seq(
        Literal(PROVENANCE_ID_FIELD), MonotonicallyIncreasingID(),
        Literal(PROVENANCE_ID_FIELD + "__CONST"), Literal("1")
      )),
      PROVENANCE_ID_STRUCT
    )()
  }



}
