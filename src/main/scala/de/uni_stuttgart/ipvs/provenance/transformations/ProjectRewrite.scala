package de.uni_stuttgart.ipvs.provenance.transformations

import java.sql.SQLSyntaxErrorException

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.{SchemaBackTrace, SchemaBackTraceNew}
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Join, Project}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable

object ProjectRewrite {
  def apply(project: Project, oid: Int)  = new ProjectRewrite(project, oid)
}

class ProjectRewrite(project: Project, oid: Int) extends UnaryTransformationRewrite(project, oid){

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

  override protected def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    val newRoot = schemaSubsetTree.rootNode
    val exprToName = scala.collection.mutable.Map[Expression,String]()
    val inNameToOutName = scala.collection.mutable.Map[String,String]()

    child match {
      // TODO: Flatten comes as Project (better way to analyze)
      case g: Generate => SchemaBackTraceNew(schemaSubsetTree).unrestructureGenerate(g, newRoot)
      /*
        TODO: Join may come as Project (better way to analyze)
              For now, we define the plan as a Join if the plan is Project with no renaming and the child is Join
       */
      case j: Join => {
        var projList = List[String]()

        for(ar <- j.left.output)
          if (ar.isInstanceOf[AttributeReference] && !projList.contains(ar.name))
            projList = projList :+ ar.name

        for(ar <- j.right.output)
          if (ar.isInstanceOf[AttributeReference] && !projList.contains(ar.name))
            projList = projList :+ ar.name

        if(projList.size == project.projectList.size)
          schemaSubsetTree.deepCopy()
        else {
          SchemaBackTraceNew(schemaSubsetTree).unrestructureProject(project, newRoot, exprToName, inNameToOutName)
        }
      }
      case _ => SchemaBackTraceNew(schemaSubsetTree).unrestructureProject(project, newRoot, exprToName, inNameToOutName)
    }

    schemaSubsetTree
  }


}
