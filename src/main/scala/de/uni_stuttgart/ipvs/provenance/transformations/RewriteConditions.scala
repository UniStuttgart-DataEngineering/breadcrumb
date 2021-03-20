package de.uni_stuttgart.ipvs.provenance.transformations

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}

object RewriteConditons {

  def apply(e: Expression, a: Attribute): Expression =
    e match {
      case op: LessThan =>
        LessThan(
          a,
          Literal("5")
        )

      case op: LessThanOrEqual =>
        LessThanOrEqual(
          a,
          Literal("4")
        )

      case op: GreaterThan =>
        GreaterThan(
          a,
          Literal("0")
        )

      case op: GreaterThanOrEqual =>
        GreaterThanOrEqual(
          a,
          Literal("3")
        )
    }
}
