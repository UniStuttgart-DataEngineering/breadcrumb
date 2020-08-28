package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BinaryExpression, Cast, CreateNamedStruct, EqualNullSafe, EqualTo, Expression, ExtractValue, GetStructField, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable


object SchemaAlternativesExpressionAlternatives{
  def apply(inputWhyNotQuestion: PrimarySchemaSubsetTree, inputPlan: LogicalPlan,
            modificationExpressions: Seq[Expression]) = {
    new SchemaAlternativesExpressionAlternatives(inputWhyNotQuestion, inputPlan, modificationExpressions)
  }
}

class SchemaAlternativesExpressionAlternatives(inputWhyNotQuestion: PrimarySchemaSubsetTree, inputPlan: LogicalPlan, modificationExpressions: Seq[Expression]) {

  var currentInputNode = inputWhyNotQuestion.getRootNode

  var directChildOfAlias = false
  var generateAccess = false
  var inside_aggregation_function = false

  def forwardTraceExpressions(): Seq[Expression] = {
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    for (expression <- modificationExpressions){
      alternativeExpressions ++= forwardTraceExpression(expression)
      assert(currentInputNode == inputWhyNotQuestion.rootNode)
    }
    alternativeExpressions.toList
  }

  def forwardTraceNamedExpressions(): Seq[NamedExpression] = {
    forwardTraceExpressions().map(ex => ex.asInstanceOf[NamedExpression])
  }

  def forwardTraceGenerator(inputExpressions: Seq[Expression], outputAttributes: Seq[Attribute]): (Seq[Expression], Seq[Attribute]) = {
    //We consider one input attribute and one output attribute only
    val inputExpression = inputExpressions.head
    val outputAttribute = outputAttributes.head
    val inputAlternatives = forwardTraceExpression(inputExpression)

    val outputAlternatives = mutable.ListBuffer.empty[Attribute]

    for ((input, tree) <- inputAlternatives zip inputWhyNotQuestion.getAllAlternatives()){
      val alternativeName = tree match {
        case input: PrimarySchemaSubsetTree => outputAttribute.name
        case _ => Constants.getAlternativeFieldName(outputAttribute.name, 0, tree.id)
      }
      outputAlternatives += AttributeReference(alternativeName, outputAttribute.dataType)()
    }
    (inputAlternatives, outputAlternatives.toList)
  }

  def forwardTraceExpression(expression: Expression): Seq[Expression] = {
    expression match {

      case a: Alias => {
        forwardTraceAlias(a)
      }
      case cns: CreateNamedStruct => {
        forwardTraceNamedStruct(cns)
      }
      case a: AttributeReference => {
        forwardTraceAttribute(a) // an attribute reference is also an attribute, thus no special case needed
      }
      case gs: GetStructField => {
        forwardTraceStructField(gs)
      }
      case l: Literal => {
        forwardTraceLiteral(l)
      }
      case b: BinaryExpression => {
        forwardTraceBinaryExpression(b)
      }
      case c: Cast => {
        forwardTraceCast(c)
      }
      case nn: IsNotNull => {
        forwardTraceIsNotNull(nn)
      }

      /*
      case ag: AggregateExpression => {
        //forwardTraceAggregateExpression(ag)
      }*/
    }

  }

  def forwardTraceAlias(a: Alias): Seq[Expression] = {
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    val childExpressions = forwardTraceExpression(a.child)
    for ((child, tree) <- childExpressions zip inputWhyNotQuestion.getAllAlternatives()){
      val alternativeName = tree match {
        case tree: PrimarySchemaSubsetTree => a.name
        case _ => Constants.getAlternativeFieldName(a.name, 0, tree.id)
      }
      //val alternativeName = Constants.getAlternativeFieldName(a.name, 0, tree.id)
      val newAlias = Alias(child, alternativeName)()
      alternativeExpressions += newAlias
    }
    alternativeExpressions.toList
  }

  def forwardTraceAttribute(attribute: AttributeReference): Seq[Expression] = {
    currentInputNode = currentInputNode.getChildren.find(node => node.name == attribute.name).get
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    for (alternative <- currentInputNode.getAllAlternatives()){

      //val alternativeAttribute = AttributeReference(alternative.name, attribute.dataType, attribute.nullable)()
      val alternativeAttribute = inputPlan.resolve(Seq(alternative.name), org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution).get
      // ExtractValue(e, Literal(name), resolver)
      // ExtractValue(parentAttributeReference, Literal(AttributeName), resolver)
      // address1.year ==> "address1" ==> getStructField, "year" ==> AttributeReference
      //val alternativeAttribute2 = ExtractValue(null, Literal(alternative.name), org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution)
      //val alternativeAttribute3 = inputPlan.resolveChildren()
      alternativeExpressions += alternativeAttribute
    }
    currentInputNode = currentInputNode.getParent()
    alternativeExpressions.toList
  }

  def forwardTraceStructFieldInternal(field: GetStructField): Seq[Expression] = {

    val childExpressions = field.child match {
      case gs: GetStructField => {
        forwardTraceStructFieldInternal(gs)
      }
      case ar: AttributeReference => {
        val expressions = forwardTraceAttribute(ar)
        currentInputNode = currentInputNode.getChildren.find(node => node.name == ar.name).get
        expressions
      }
    }
    currentInputNode = currentInputNode.getChildren.find(node => node.name == field.name.get).get
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    for ((alternative, expression) <- currentInputNode.getAllAlternatives() zip childExpressions)
    {

      //val exp1 = GetStructField(expression, field.ordinal, Some(alternative.name))
      val alternativeExpression = ExtractValue(expression, Literal(alternative.name), org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution)
      alternativeExpressions += alternativeExpression
    }
    alternativeExpressions.toList

  }

  def forwardTraceStructField(field: GetStructField): Seq[Expression] = {
    val initialInputNode = currentInputNode
    val expressions = forwardTraceStructFieldInternal(field)
    currentInputNode = initialInputNode
    expressions
  }

  def forwardTraceLiteral(literal: Literal): Seq[Expression] = {
    inputWhyNotQuestion.getAllAlternatives().map{_ => literal.copy()}
  }

  def forwardTraceNamedStruct(cns: CreateNamedStruct): Seq[Expression] = {
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    val alternativeChildExpressions = mutable.ListBuffer.empty[Seq[Expression]]
    for (child <- cns.children) {
      alternativeChildExpressions += forwardTraceExpression(child)
    }
    for (idx <- 0 until inputWhyNotQuestion.getAllAlternatives().size){
      val newChildExpressions = mutable.ListBuffer.empty[Expression]
      for (expressionList <- alternativeChildExpressions){
        newChildExpressions += expressionList(idx)
      }
      alternativeExpressions += CreateNamedStruct(newChildExpressions)
    }
    alternativeExpressions
  }

  def forwardTraceBinaryExpression(expression: BinaryExpression): Seq[Expression] = {
    val leftAlternativeExpressions = forwardTraceExpression(expression.left)
    val rightAlternativeExpressions = forwardTraceExpression(expression.right)
    val currentNode = currentInputNode
    //TODO: expression.withNewChildren()
    val outputExpressions = expression match {
      case _: EqualTo => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => EqualTo(left, right)
        }
      }
      case _: EqualNullSafe => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => EqualNullSafe(left, right)
        }
      }
      case _: LessThan => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => LessThan(left, right)
        }
      }
      case _: LessThanOrEqual => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => LessThanOrEqual(left, right)
        }
      }
      case _: GreaterThan => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => GreaterThan(left, right)
        }
      }
      case _: GreaterThanOrEqual => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => GreaterThanOrEqual(left, right)
        }
      }
    }
    assert(currentNode == currentInputNode)
    outputExpressions
  }

  def forwardTraceCast(cast: Cast): Seq[Expression] = {
    val alternativeExpressions = forwardTraceExpression(cast.child)
    alternativeExpressions.map {
      expression => Cast(expression, cast.dataType)
    }
  }

  def forwardTraceIsNotNull(nn: IsNotNull): Seq[Expression] = {
    val alternativeExpressions = forwardTraceExpression(nn.child)
    alternativeExpressions.map {
      expression => IsNotNull(expression)
    }

  }

}
