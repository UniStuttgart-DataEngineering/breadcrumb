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

  def compatibleColumn(child: LogicalPlan, provenanceContext: ProvenanceContext): NamedExpression = {
    val udfExpression = getDataFetcherExpression(child)
    val attributeName = Constants.getCompatibleFieldName(oid)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(udfExpression,attributeName)()
  }

  def getDataFetcherExpression(child: LogicalPlan) = {
    val udf = ProvenanceContext.getUDF
    val children = ArrayBuffer[Expression](getNamedStructExpression(child.output), whyNotQuestion.getSchemaSubsetTreeExpression)
    val inputIsNullSafe = true :: true :: Nil
    val inputTypes = udf.inputTypes.getOrElse(Seq.empty[DataType])
    val udfName = Some(Constants.getUDFName)
    ScalaUDF(udf.f, udf.dataType, children, inputIsNullSafe, inputTypes, udfName)
  }

  def getNamedStructExpression(output: Seq[Expression]): Expression = {
    CreateStruct(output)
  }

  def compatibleColumnDepricated(provenanceContext: ProvenanceContext): NamedExpression = {
    //TODO: integrate with unrestructured whyNot question
    val attributeName = Constants.getCompatibleFieldName(oid)
    provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    val condition = LessThanOrEqual(Rand(Literal(42)), Literal(0.5))
    val elseValue: Option[Expression] = Some(Literal(false))
    val branches: Seq[(Expression, Expression)] = Seq(Tuple2(condition, Literal(true)))
    Alias(CaseWhen(branches, elseValue), attributeName)()
  }


  override def rewrite: Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val projectList = relation.output :+ compatibleColumn(relation, provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )
    //TODO: resolve compatible columns
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
