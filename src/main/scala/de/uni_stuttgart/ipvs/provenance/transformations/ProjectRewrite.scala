package de.uni_stuttgart.ipvs.provenance.transformations

import java.sql.SQLSyntaxErrorException

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaAlternativesExpressionAlternatives, SchemaAlternativesForwardTracing, SchemaNode, SchemaSubsetTree, SchemaSubsetTreeBackTracing}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTrace
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Join, Project}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable

object ProjectRewrite {
  def apply(project: Project, oid: Int)  = new ProjectRewrite(project, oid)
}

class ProjectRewrite(project: Project, oid: Int) extends UnaryTransformationRewrite(project, oid){

  @deprecated
  //when none is returned, the attribute referenced is not part of the schemasubset, we look at ;)
  def outputNode(ex: Expression, currentSchemaNodeOption: Option[SchemaNode]): Option[SchemaNode] = {
    if (!currentSchemaNodeOption.isDefined) return None
    val currentSchemaNode = currentSchemaNodeOption.get
    var returnNode: Option[SchemaNode] = None
    ex match {
      case a: Alias => {
        //nothing to do here, because the alias matches the
        returnNode = outputNode(a.child, currentSchemaNodeOption)
      }
      case ar: AttributeReference => {
        returnNode = currentSchemaNode.getChild(ar.name)
      }
      case a: Attribute => {
        returnNode = currentSchemaNode.getChild(a.name)
      }
      case ma: MultiAlias => {
        //maybe we do not want to support this at the beginning?
        throw new SQLSyntaxErrorException("MultiAlias expresssion " + ma.toString() + "is not supported with WhyNot Provenance, yet.")
      }
      case ua: UnresolvedAlias => {
        //maybe we do not want to support this at the beginning?
        throw new SQLSyntaxErrorException("UnresolvedAlias expresssion " + ua.toString() + "is not supported with WhyNot Provenance, yet.")
      }
      case ex: Expression => {
        throw new SQLSyntaxErrorException("Expression " + ex.toString() + "is not supported with WhyNot Provenance, yet.")
      }
    }
    returnNode
  }

  @deprecated
  def inputNode(ex: Expression, currentSchemaNode: SchemaNode, nodeToBeAdded:SchemaNode): Unit = {
    ex match {
      case a: Alias => {
        nodeToBeAdded.rename(a.name)
        currentSchemaNode.addChild(nodeToBeAdded)
      }
      case ar: AttributeReference => {
        currentSchemaNode.addChild(nodeToBeAdded)
      }
      case a: Attribute => {
        currentSchemaNode.addChild(nodeToBeAdded)
      }
    }
  }

  override def rewrite: Rewrite = {
    //val childRewrite = WhyNotPlanRewriter.rewrite(project.child, SchemaBackTrace(project, whyNotQuestion).unrestructure().head)
    val childRewrite = child.rewrite()
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext

    val projectList = project.projectList ++ provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output)
    val rewrittenProjection = Project(projectList, rewrittenChild)
    Rewrite(rewrittenProjection, provenanceContext)
  }

  override protected[provenance] def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    SchemaSubsetTreeBackTracing(schemaSubsetTree, child.plan.output, project.output, project.projectList).getInputTree()
  }

  override def rewriteWithAlternatives(): Rewrite = {
    val childRewrite = child.rewriteWithAlternatives()
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext
    val alternativeExpressions = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, rewrittenChild, project.projectList).forwardTraceNamedExpressions()

    //val addressAttributes = rewrittenChild.output.find(attr => attr.name == "address").toSeq
    //val projectList = addressAttributes ++ alternativeExpressions ++ provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output)

    //TODO: create alternative renames --> should be done now
    val projectList = alternativeExpressions ++ provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output)
    //val projectList = addressAttributes ++ alternativeExpressions ++ provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output)
    val rewrittenProjection = Project(projectList, rewrittenChild)
    //TODO: update alternative schema trees --> should be done now, too
    val outputTrees = SchemaAlternativesForwardTracing(provenanceContext.primarySchemaAlternative, rewrittenChild, project.projectList).forwardTraceExpressions().getOutputWhyNotQuestion()
    provenanceContext.primarySchemaAlternative = outputTrees
    Rewrite(rewrittenProjection, provenanceContext)

  }
}
