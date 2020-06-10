package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaMatch
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, CaseWhen, CreateNamedStruct, Expression, GetStructField, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, NamedExpression, Or, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, Project}
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

import scala.collection.mutable


object RelationRewrite {
  def apply(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new RelationRewrite(relation, whyNotQuestion, oid)
}

class RelationRewrite(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int) extends InputTransformationRewrite(relation, whyNotQuestion, oid){

  def compatibleColumn(provenanceContext: ProvenanceContext): NamedExpression = {
    //TODO: integrate with unrestructured whyNot question
    val attributeName = Constants.getCompatibleFieldName(oid)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    val condition = LessThanOrEqual(Rand(Literal(42)), Literal(0.5))
    val elseValue: Option[Expression] = Some(Literal(false))
    val branches: Seq[(Expression, Expression)] = Seq(Tuple2(condition, Literal(true)))
    Alias(CaseWhen(branches, elseValue), attributeName)()

    /*
    // Collect attributes from base relation
    val AttrNameToRef = scala.collection.mutable.Map[String, Expression]()
    for (attr <- relation.output) {
      AttrNameToRef.put(attr.name, attr)
    }

    var condition = List[Expression]()
    var constant: String = null
//    val condition = LessThanOrEqual(Rand(Literal(42)), Literal(0.5))
//    val branches: Seq[(Expression, Expression)] = Seq(Tuple2(condition, Literal(true)))

    // Adapt the condition from the why-not question
    for (child <- whyNotQuestion.rootNode.children) {
      if (child.constraint.constraintString == "") {
        condition = condition :+ Literal(true)
      } else {
        val conditionInChild = child.constraint.constraintString

        if (conditionInChild.contains(">")) {
          constant = conditionInChild.substring(2)
          condition = condition :+ expressions.GreaterThan(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.contains(">=")) {
          constant = conditionInChild.substring(3)
          condition = condition :+ GreaterThanOrEqual(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.contains("<")) {
          constant = conditionInChild.substring(2)
          condition = condition :+ LessThan(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.contains("<=")) {
          constant = conditionInChild.substring(3)
          condition = condition :+ LessThanOrEqual(AttrNameToRef.get(child.name).get, Literal(constant))
        } else if (conditionInChild.equals("=")) {
          constant = conditionInChild.substring(2)
//          condition = Equal(Literal(child.name), Literal(constant))
        }
      }
    }

//    val elseValue: Option[Expression] = Some(Literal(false))
//    Alias(CaseWhen(branches, elseValue), attributeName)()

      var conjunctCond: Expression = null
      if (condition.length == 1) {
        conjunctCond = condition.head
      } else {
        for (eachCond <- condition) {
          conjunctCond = Or(conjunctCond,eachCond)
        }
      }

      Alias(conjunctCond, attributeName) ()

     */
  }

  override def rewrite: Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val projectList = relation.output :+ compatibleColumn(provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )

    //TODO: resolve compatible columns

    Rewrite(rewrittenLocalRelation, provenanceContext)
  }


  var unrestructuredWhyNotQuestionInput: SchemaSubsetTree = null
  var unrestructuredWhyNotQuestionOutput: SchemaSubsetTree = whyNotQuestion


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
    unrestructuredWhyNotQuestionInput = whyNotQuestion.deepCopy()

    var newRoot = whyNotQuestion.rootNode.deepCopy(null)
    newRoot.children.clear()
    unrestructuredWhyNotQuestionInput.rootNode = newRoot
    val exprToName = scala.collection.mutable.Map[Expression,String]()

    for (ex <- relation.schema) {
      ex match {
        case ar: StructField => {
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
        case _ => // do nothing
      }
    }

    unrestructuredWhyNotQuestionInput
  }


}
