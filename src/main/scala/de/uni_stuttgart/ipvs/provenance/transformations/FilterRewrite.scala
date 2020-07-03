package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTraceNew
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Expression, NamedExpression, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.BooleanType

import scala.collection.mutable.ListBuffer

object FilterRewrite {
  def apply(filter: Filter, oid: Int)  = new FilterRewrite(filter: Filter, oid: Int)
}

class FilterRewrite(filter: Filter, oid: Int) extends UnaryTransformationRewrite(filter, oid) {

  override def rewrite: Rewrite = {
    //val childRewrite = WhyNotPlanRewriter.rewrite(filter.child, SchemaBackTrace(filter, whyNotQuestion).unrestructure().head)
    val childRewrite = child.rewrite()


    val provenanceContext = childRewrite.provenanceContext
    val rewrittenChild = childRewrite.plan

    val projectList = filter.output ++
      provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output) ++
      provenanceAttributes(childRewrite)
    val rewrittenFilter = Project(
      projectList,
      childRewrite.plan
    )

    Rewrite(rewrittenFilter, childRewrite.provenanceContext)
  }

  def provenanceAttributes(rewrite: Rewrite): Seq[NamedExpression] = {
    val attributesToBeAdded = ListBuffer.empty[NamedExpression]
    attributesToBeAdded += survivorColumn(rewrite)
    attributesToBeAdded += compatibleColumn(rewrite)
    attributesToBeAdded.toList
  }

  //TODO maybe move to abstract class, change to provenance class
  def compatibleColumn(rewrite: Rewrite): NamedExpression = {
    val lastCompatibleAttribute = getPreviousCompatible(rewrite)
    val attributeName = addCompatibleAttributeToProvenanceContext(rewrite.provenanceContext)
    Alias(lastCompatibleAttribute, attributeName)()
  }

  def survivorColumn(rewrite: Rewrite): NamedExpression = {
    val attributeName = Constants.getSurvivorFieldName(oid)
    rewrite.provenanceContext.addSurvivorAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(filter.condition, attributeName)()
  }

  override protected def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    val newRoot = schemaSubsetTree.rootNode

    // Filter may occur over another operator or base relation
    child match {
      case l: LogicalRelation => {
        SchemaBackTraceNew(schemaSubsetTree).unrestructureLeaf(l, newRoot, null)
        schemaSubsetTree
      }
      case _ => schemaSubsetTree.deepCopy()
    }
  }





}
