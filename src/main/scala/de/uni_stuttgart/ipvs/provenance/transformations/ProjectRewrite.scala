package de.uni_stuttgart.ipvs.provenance.transformations

import java.sql.SQLSyntaxErrorException

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ProjectRewrite {
  def apply(project: Project, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new ProjectRewrite(project, whyNotQuestion, oid)
}

class ProjectRewrite(project: Project, whyNotQuestion:SchemaSubsetTree, oid: Int) extends UnaryTransformationRewrite(project, whyNotQuestion, oid){

  var unrestructuredWhyNotQuestionInput: SchemaSubsetTree = null
  var unrestructuredWhyNotQuestionOutput: SchemaSubsetTree = whyNotQuestion

  def unrestructureSubExpressions(expressions: Seq[Expression], renamedNode: SchemaNode,
                                  newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]): SchemaNode = {
    for (eachExp <- expressions) {
      eachExp match {
        case lt: Literal => // do nothing
        case gsf: GetStructField => {
          unrestructureSubExpressions(gsf.children, renamedNode, newRoot, exprToName)
        }
        case ar: AttributeReference => {
          // Create a new node
          val newNode: SchemaNode = SchemaNode(ar.name)

          /*
            1) Create a node "A" corresponding to the original attribute from the input
            2) Correctly create or copy all the descendants of "A"
            3) Add the tree rooted "A" to the new tree for the why-not question
           */
          if (renamedNode.children.isEmpty) {
            val leafNode = newRoot.getLeafNode()

            if (!leafNode.name.equals(newRoot.name)) {
              leafNode.addChild(newNode)
              newNode.setParent(leafNode)
              newNode.copyNode(leafNode)
            }
          } else {
            val node = renamedNode.getChild(ar.name).getOrElse(null)

            if (node != null) {
              newNode.copyNode(node)
            } else {
              newNode.name = ""

              val doubleCheck = exprToName(ar)
              if (doubleCheck != "" && !doubleCheck.equals(ar.name)) {
                val doubleCheckNode = renamedNode.getChild(doubleCheck).getOrElse(null)
                renamedNode.copyNode(doubleCheckNode)
                val node = renamedNode.getChild(ar.name).getOrElse(null)

                if (node != null) {
                  newNode.copyNode(doubleCheckNode)
                } else {
                  newNode.name = ar.name
                }
              }
            }
          }

          if (newNode.name != "") {
            if (ar.dataType.typeName.equals("struct")) {
              getAllStructFields(ar.dataType.asInstanceOf[StructType], newNode)
            }

            newRoot.addChild(newNode)
            newNode.setParent(newRoot)
          }
        }
        case cns: CreateNamedStruct => {
          var pos = 0

          // Map expression to its name (inner Alias is ignored in the plan)
          for (each <- cns.valExprs) {
            val name = cns.names(pos).toString
            exprToName.put(each,name)

            // Further collect descendant if the attribute is nested
            each match {
              case gsf: GetStructField => exprToName.put(gsf.child, gsf.name.get)
              case _ => // do nothing
            }
            pos += 1
          }

          // Access to the inner renamed child node (attribute)
          if (pos > 0) {
            val name = exprToName.get(cns).getOrElse("")

            if (!name.equals("")) {
              val node = renamedNode.getChild(name).getOrElse(null)
              if (node != null) renamedNode.copyNode(node)
            }
          }

          unrestructureSubExpressions(cns.valExprs, renamedNode, newRoot, exprToName)
        }
      }
    }

    newRoot
  }


  def getAllStructFields(st: StructType, newNode: SchemaNode): SchemaNode = {
    for (eachStruct <- st) {
      val structNode = SchemaNode("")

      if (eachStruct.dataType.typeName.equals("struct")) {
        getAllStructFields(eachStruct.dataType.asInstanceOf[StructType], structNode)
      }

      structNode.name = eachStruct.name
      newNode.addChild(structNode)
      structNode.setParent(newNode)
    }

    newNode
  }


  override def unrestructure(): SchemaSubsetTree = {
    //TODO this is not correct
    //4 cases:
    // 0) selection: a --> a
    // 1) renaming: a --> b
    // 2) tuple unnesting c<a,b> --> a --> d
    // 3) tuple nesting a, b -> c<a,b>

//    val unrestructuredWhyNotQuestion = whyNotQuestion.deepCopy()
    unrestructuredWhyNotQuestionInput = whyNotQuestion.deepCopy()

    var newRoot = whyNotQuestion.rootNode.deepCopy(null)
    newRoot.children.clear()
    unrestructuredWhyNotQuestionInput.rootNode = newRoot
    val exprToName = scala.collection.mutable.Map[Expression,String]()

    for (ex <- project.projectList) {
      ex match {
        case ar: AttributeReference => {
          // Copy the node from why-not question
          val node = whyNotQuestion.getNodeByName(ar.name)
          val newNode: SchemaNode = SchemaNode(ar.name)

          if (node != null) {
//            newNode.copyNode(node)

            if (ar.dataType.typeName.equals("struct")) {
              newNode.deepCopy(node)

              if (node.children.isEmpty) {
                getAllStructFields(ar.dataType.asInstanceOf[StructType], newNode)
              } else {
                val fields = ar.dataType.asInstanceOf[StructType]
                var newFields = Array[StructField]()

                for (f <- fields) {
                  val childName = node.children.find(node => node.name == f.name).getOrElse("")
                  if (!childName.equals("")) newFields = newFields :+ f
                }

                getAllStructFields(StructType(newFields), newNode)
              }
            } else {
              newNode.copyNode(node)
            }

            // Add to newRoot
            newRoot.addChild(newNode)
            newNode.setParent(newRoot)
          }
        }
        case a: Alias => {
          a.name //rename
          //a.child.asInstanceOf[AttributeReference].name
          //child.ge
          //println(child.prettyName + ": " + name)

          // Access the node corresponding to the renamed attribute in the why-not question
          val node = whyNotQuestion.getNodeByName(a.name)
          /*
            Parameters:
              1) Sub-expressions in projection operator
              2) A tree that the root is the attribute corresponding to the renamed attribute
              3) A new tree for creating the unrestructured why-not question
              4) A map: key = an expression and value = the name for the expression
            How it works:
              1) Recursively access child nodes in the tree for the why-not question to find any renamed and restructured attribute
              2) Create a new tree for the why-not question based on the sub-expressions from the logical plan as well as why-not question schema
           */
          unrestructureSubExpressions(a.children, node, newRoot, exprToName)
        }
        case MultiAlias(child, names) => {

        }
        case UnresolvedAlias(child, aliasFunc) => {
          //maybe we do not want to support this at the beginning?
        }
      }
    }

    unrestructuredWhyNotQuestionInput
  }

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
    unrestructuredWhyNotQuestionOutput = unrestructure()
    val childRewrite = WhyNotPlanRewriter.rewrite(project.child, unrestructuredWhyNotQuestionOutput)
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext

    val projectList = project.projectList ++ provenanceContext.getExpressionFromAllProvenanceAttributes(rewrittenChild.output)
    val rewrittenProjection = Project(projectList, rewrittenChild)
    Rewrite(rewrittenProjection, provenanceContext)
  }


}
