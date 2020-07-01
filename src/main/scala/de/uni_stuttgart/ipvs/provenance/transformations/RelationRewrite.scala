package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaNode, SchemaSubsetTree}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CaseWhen, CreateStruct, Expression, LessThanOrEqual, Literal, NamedExpression, Rand, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.types.{BooleanType, DataType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


object RelationRewrite {
  def apply(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new RelationRewrite(relation, whyNotQuestion, oid)
}

class RelationRewrite(relation: LeafNode, whyNotQuestion:SchemaSubsetTree, oid: Int) extends InputTransformationRewrite(relation, whyNotQuestion, oid){




  override def rewrite: Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val projectList = relation.output :+ compatibleColumn(relation, provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )
    Rewrite(rewrittenLocalRelation, provenanceContext)
  }


  var unrestructuredWhyNotQuestionInput: SchemaSubsetTree = null

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

  def unrestructureFilter(st: StructField, newRoot: SchemaNode): SchemaNode = {
    val node = whyNotQuestion.getNodeByName(st.name)
    val newNode: SchemaNode = SchemaNode(st.name)

    if (node != null) {
      if (st.dataType.typeName.equals("struct")) {
        newNode.deepCopy(node)

        if (node.children.isEmpty) {
          getAllStructFields(st.dataType.asInstanceOf[StructType], newNode)
        } else {
          val fields = st.dataType.asInstanceOf[StructType]
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

    newRoot
  }

  override def unrestructure(): SchemaSubsetTree = {
    unrestructuredWhyNotQuestionInput = whyNotQuestion.deepCopy()

    var newRoot = whyNotQuestion.rootNode.deepCopy(null)
    newRoot.children.clear()
    unrestructuredWhyNotQuestionInput.rootNode = newRoot
    val exprToName = scala.collection.mutable.Map[Expression,String]()

    for (ex <- relation.schema) {
      ex match {
        case st: StructField => unrestructureFilter(st, newRoot)
        case _ => // do nothing
      }
    }

    unrestructuredWhyNotQuestionInput
  }


}
