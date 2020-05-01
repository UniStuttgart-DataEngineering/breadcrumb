package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.StructType

object ProjectRewrite {
  def apply(project: Project, whyNotQuestion:SchemaMatch, oid: Int)  = new ProjectRewrite(project: Project, whyNotQuestion:SchemaMatch, oid: Int)
}

class ProjectRewrite(project: Project, whyNotQuestion:SchemaMatch, oid: Int) extends TransformationRewrite(project, whyNotQuestion, oid){

  override def unrestructure(child: Option[LogicalPlan] = None): SchemaMatch = {

    //TODO this is not correct
    //3 cases:
    // 1) renaming: a --> b
    // 2) tuple unnesting c<a,b> --> a --> d
    // 2) tuple nesting a, b -> c<a,b>
    whyNotQuestion
  }

  override def rewrite: Rewrite = {
    val childRewrite = WhyNotPlanRewriter.rewrite(project.child, unrestructure())
    val rewrittenChild = childRewrite.plan
    val addedProvenance = childRewrite.provenanceExtension

    val provenanceAttributeOption = rewrittenChild.output.
      collectFirst{case attr if (attr.name == Constants.PROVENANCE_ID_STRUCT) => attr}


    val projectList = project.projectList ++ provenanceAttributeOption
    val rewrittenProjection = Project(projectList, rewrittenChild)
    Rewrite(rewrittenProjection, addedProvenance)
  }


}
