package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BinaryExpression, Cast, CreateNamedStruct, EqualNullSafe, EqualTo, Expression, GetStructField, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable


object SchemaAlternativesForwardTracing{
  def apply(inputWhyNotQuestion: PrimarySchemaSubsetTree, inputPlan: LogicalPlan,
            modificationExpressions: Seq[Expression]) = {
    new SchemaAlternativesForwardTracing(inputWhyNotQuestion, inputPlan, modificationExpressions)
  }
}

class SchemaAlternativesForwardTracing(inputWhyNotQuestion: PrimarySchemaSubsetTree, inputPlan: LogicalPlan, modificationExpressions: Seq[Expression]) {

  var currentInputNode = inputWhyNotQuestion.getRootNode

  var currentLeftInputNode = inputWhyNotQuestion.getRootNode
  var currentRightInputNode = inputWhyNotQuestion.getRootNode

  var directChildOfAlias = false
  var generateAccess = false
  var inside_aggregation_function = false

  val inToOutAttr = scala.collection.mutable.Map[String,String]()
  var unionAccess = false

  def forwardTraceExpressions(): Unit = {
    for (expression <- modificationExpressions){
      forwardTraceExpression(expression)
      assert(currentInputNode == inputWhyNotQuestion.rootNode)
    }
  }

  def forwardTraceExpression(expression: Expression): Seq[Expression] = {
    expression match {
        /*
      case a: Alias => {
        //forwardTraceAlias(a)
      }
      case cns: CreateNamedStruct => {
        //forwardTraceNamedStruct(cns)
      }*/
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
      /*
      case ag: AggregateExpression => {
        //forwardTraceAggregateExpression(ag)
      }*/
    }

  }

  def forwardTraceAttribute(attribute: AttributeReference): Seq[Expression] = {
    currentInputNode = currentInputNode.getChildren.find(node => node.name == attribute.name).get
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    for (alternative <- currentInputNode.alternatives){
      //val alternativeAttribute = AttributeReference(alternative.name, attribute.dataType, attribute.nullable)()
      val alternativeAttribute = inputPlan.resolve(Seq(alternative.name), org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution).get
      alternativeExpressions += alternativeAttribute
    }
    currentInputNode = currentInputNode.getParent()
    alternativeExpressions.toList
  }

  def forwardTraceStructField(field: GetStructField): Seq[Expression] = {
    val alternativeExpressions = mutable.ListBuffer.empty[Expression]
    currentInputNode = currentInputNode.getChildren.find(node => node.name == field.name).get
    val childExpressions = forwardTraceExpression(field.child)
    for ((alternative, expression) <- currentInputNode.alternatives zip childExpressions)
    {
      alternativeExpressions += GetStructField(expression, field.ordinal, Some(alternative.name))
    }
    currentInputNode = currentInputNode.getParent()
    alternativeExpressions.toList
  }

  def forwardTraceLiteral(literal: Literal): Seq[Expression] = {
    inputWhyNotQuestion.alternatives.map{_ => literal.copy()}
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

}
