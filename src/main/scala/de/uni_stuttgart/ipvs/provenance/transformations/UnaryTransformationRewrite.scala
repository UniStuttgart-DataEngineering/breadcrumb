package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnaryNode}

abstract class UnaryTransformationRewrite(val plan: UnaryNode, val oid: Int) extends TransformationRewrite {
  val child: TransformationRewrite = WhyNotPlanRewriter.buildRewriteTree(plan.child)

  def children: Seq[TransformationRewrite] = child :: Nil

  override protected def backtraceChildrenWhyNotQuestion: Unit = {
    val inputWhyNotQuestion = undoSchemaModifications(whyNotQuestion)
    child.backtraceWhyNotQuestion(inputWhyNotQuestion)
  }

  def renameValidColumns(validColumns: Seq[NamedExpression]): Seq[NamedExpression] = {
    validColumns.map{
      attribute => {
        val attributeName = Constants.getValidFieldWithOldExtension(attribute.name)
        Alias(attribute, attributeName)()
      }
    }
  }

  def planWithOldValidFields(childRewrite: Rewrite): (LogicalPlan, Seq[String]) = {
    val oldValidColumns = childRewrite.provenanceContext.getExpressionsFromProvenanceAttributes(childRewrite.provenanceContext.getMostRecentCompatibilityAttributes(), childRewrite.plan.output)
    val remainingAttributes = childRewrite.plan.output.filterNot(attribute => oldValidColumns.contains(attribute))
    val renamedAttributes = renameValidColumns(oldValidColumns)
    val projectList = remainingAttributes ++ renamedAttributes
    val newPlan = Project(projectList, childRewrite.plan)
    (newPlan, renamedAttributes.map(attr => attr.name))
  }

  protected def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree

}
