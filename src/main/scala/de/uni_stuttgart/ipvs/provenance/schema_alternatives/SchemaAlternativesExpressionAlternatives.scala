package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, And, Attribute, AttributeReference, BinaryExpression, Cast, Contains, CreateNamedStruct, DayOfMonth, Divide, EqualNullSafe, EqualTo, Expression, ExtractValue, FromUnixTime, GetStructField, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, Literal, Multiply, NamedExpression, Not, Or, ParseToDate, Size, Subtract}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, CollectList, Count, Max, Min, Sum}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StringType

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
  var forGroupingSets = false

  var withAlternatives = true

  def forwardTraceExpressions(): Seq[Expression] = {
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    for (expression <- modificationExpressions){
      withAlternatives = true
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

  def forwardTraceJoinExpression(expression: Expression): (Expression, Seq[Expression]) = {
    val alternativeExpressions = forwardTraceExpression(expression)
    var aggregatedExpression = alternativeExpressions(0)
    /*
    for (alternativeExpression <- alternativeExpressions.tail){
      aggregatedExpression = Or(aggregatedExpression, alternativeExpression)
    }*/
    (aggregatedExpression, alternativeExpressions)
  }

  def forwardTracDayOfMonth(d: DayOfMonth): Seq[Expression] = {
    val alternativeExpressions = forwardTraceExpression(d.child)
    alternativeExpressions.map(DayOfMonth(_))
  }

  def forwardTraceParseToDate(p: ParseToDate): Seq[Expression] = {
    val alternativeChildren = forwardTraceExpression(p.child)
    //val alternativeLeft = forwardTraceExpression(p.left)
    alternativeChildren map {
      case child => ParseToDate(child, p.format, child)
    }
  }

  def forwardTraceMultiply(m: Multiply): Seq[Expression] = {
    val leftAlternativeExpressions = forwardTraceExpression(m.left)
    val rightAlternativeExpressions = forwardTraceExpression(m.right)

    leftAlternativeExpressions zip rightAlternativeExpressions map {
      case (left, right) => Multiply(left, right)
    }
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
      case s: Size => {
        forwardTraceSize(s)
      }
      case nn: IsNotNull => {
        forwardTraceIsNotNull(nn)
      }
      case ag: AggregateExpression => {
        forwardTraceAggregateExpression(ag)
      }
      case not: Not => {
        forwardTraceNot(not)
      }
      case d: DayOfMonth => {
        forwardTracDayOfMonth(d)
      }
      case p: ParseToDate => {
        forwardTraceParseToDate(p)
      }
      // TODO: Multiply cannot be cast to NamedExpression
      case m: Multiply => {
        forwardTraceMultiply(m)
      }

    }

  }

  def forwardTraceAlias(a: Alias): Seq[Expression] = {
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    val childExpressions = forwardTraceExpression(a.child)
    if(withAlternatives){
      for ((child, tree) <- childExpressions zip inputWhyNotQuestion.getAllAlternatives()){
        val alternativeName = tree match {
          case tree: PrimarySchemaSubsetTree => a.name
          case _ => Constants.getAlternativeFieldName(a.name, 0, tree.id)
        }
        //val alternativeName = Constants.getAlternativeFieldName(a.name, 0, tree.id)
        if(!forGroupingSets){
          val newAlias = Alias(child, alternativeName)()
          alternativeExpressions += newAlias
        } else {
          alternativeExpressions += child
        }
      }
      alternativeExpressions.toList
    } else {
      val newAlias = Alias(childExpressions.head, a.name)()
      List(newAlias)
    }


  }

  def forwardTraceAttribute(attribute: AttributeReference): Seq[Expression] = {
    currentInputNode = currentInputNode.getChildren.find(node => node.name == attribute.name).getOrElse(handleMissingNode())
    if (!withAlternatives) {
      return inputPlan.resolve(Seq(attribute.name), org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution).toSeq
    }
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

  def handleMissingNode(): PrimarySchemaNode = {
    withAlternatives = false
    currentInputNode
  }

  def forwardTraceStructFieldInternal(field: GetStructField): Seq[Expression] = {

    val childExpressions = field.child match {
      case gs: GetStructField => {
        forwardTraceStructFieldInternal(gs)
      }
      case ar: AttributeReference => {
        val expressions = forwardTraceAttribute(ar)
        if(withAlternatives){
          currentInputNode = currentInputNode.getChildren.find(node => node.name == ar.name).get
        }
        expressions
      }
    }
    currentInputNode = currentInputNode.getChildren.find(node => node.name == field.name.get).getOrElse(handleMissingNode())
    if(withAlternatives){
      val alternativeExpressions = mutable.ListBuffer.empty[Expression]
      for ((alternative, expression) <- currentInputNode.getAllAlternatives() zip childExpressions)
      {

        //val exp1 = GetStructField(expression, field.ordinal, Some(alternative.name))
        val alternativeExpression = ExtractValue(expression, Literal(alternative.name), org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution)
        alternativeExpressions += alternativeExpression
      }
      alternativeExpressions.toList
    } else {
      val alternativeExpression = ExtractValue(childExpressions.head, Literal(field.name.get), org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution)
      List(alternativeExpression)
    }


  }

  def forwardTraceStructField(field: GetStructField): Seq[Expression] = {
    val initialInputNode = currentInputNode
    val expressions = forwardTraceStructFieldInternal(field)
    currentInputNode = initialInputNode
    expressions
  }

  def isForGroupingSets() : this.type = {
    forGroupingSets = true
    this
  }

  def forwardTraceAggregateExpression(expression: AggregateExpression): Seq[Expression] = {
    val alternativeChildExpressions = forwardTraceExpression(expression.aggregateFunction.children.head)
    if (forGroupingSets) return alternativeChildExpressions
    alternativeChildExpressions.map {
      child => getAggregateFunction(expression, child)
        /*
      {
        val aggFunction = expression.aggregateFunction.withNewChildren(Seq(child))
        val aggExpression = expression.withNewChildren(Seq(aggFunction))
        aggExpression
      } */
    }
  }

  def getAggregateFunction(expression: AggregateExpression, alternative: Expression): AggregateExpression = {
    val function = expression.aggregateFunction match {
      case _: Sum => Sum(alternative)
      case _: Max => Max(alternative)
      case _: Min => Min(alternative)
      case _: Count => Count(alternative)
      case _: CollectList => CollectList(alternative)
      case _: Average => Average(alternative)
    }
    val expr = AggregateExpression(function, expression.mode, expression.isDistinct)
    expr
  }

  def forwardTraceLiteral(literal: Literal): Seq[Expression] = {
    inputWhyNotQuestion.getAllAlternatives().map{_ => literal.copy()}
  }

  def forwardTraceNamedStruct(cns: CreateNamedStruct): Seq[Expression] = {
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    val childNames = mutable.ListBuffer.empty[String]
    val alternativeChildExpressions = mutable.ListBuffer.empty[Seq[Expression]]
    for (List(name, expression) <- cns.children.grouped(2)) {
      childNames += name.asInstanceOf[Literal].value.toString
      val forwardedExpressions = forwardTraceExpression(expression)
      alternativeChildExpressions += forwardedExpressions

    }
    for ((alternative, idx) <- inputWhyNotQuestion.getAllAlternatives().zipWithIndex){
      val newChildExpressions = mutable.ListBuffer.empty[Expression]
      for ((name, expressionList) <- childNames zip alternativeChildExpressions){
        val alternativeNodeName = alternative match {
          case _: PrimarySchemaSubsetTree => name
          case _ => Constants.getAlternativeFieldName(name, 0, alternative.id)
        }
        val namedExpression = Literal(alternativeNodeName, StringType)
        newChildExpressions += namedExpression
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
      case _: Contains => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => Contains(left, right)
        }
      }
      case _: And => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => And(left, right)
        }
      }
      case f: FromUnixTime => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => {
            left.resolved
            right.resolved
            FromUnixTime(left, right, f.timeZoneId)
          }
        }
      }
      case _: Divide => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => Divide(left, right)
        }
      }
      case _: Multiply => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => Multiply(left, right)
        }
      }
      case _: Subtract => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => Subtract(left, right)
        }
      }
      case _: Add => {
        leftAlternativeExpressions zip rightAlternativeExpressions map {
          case (left, right) => Add(left, right)
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

  def forwardTraceSize(size: Size): Seq[Expression] = {
    val alternativeExpressions = forwardTraceExpression(size.child)
    alternativeExpressions.map {
      expression => Size(expression)
    }
  }

  def forwardTraceNot(not: Not): Seq[Expression] = {
    val alternativeExpressions = forwardTraceExpression(not.child)
    alternativeExpressions.map {
      expression => Not(expression)
    }
  }

  def forwardTraceIsNotNull(nn: IsNotNull): Seq[Expression] = {
    val alternativeExpressions = forwardTraceExpression(nn.child)
    alternativeExpressions.map {
      expression => IsNotNull(expression)
    }

  }

}
