package de.uni_stutde.uni_stuttgart.ipvs.provenance.transformations
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotPlanRewriter.buildAnnotation
import de.uni_stuttgart.ipvs.provenance.transformations.RewriteConditons
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Expression, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}


class FilterRewrite {
  var opNum: Int = 0
  opNum += 1

  //  Rewriting the plan
  def apply(condition: Expression, children: LogicalPlan): LogicalPlan = {
    // Selecting attribute for new condition
    // TODO: schema alternatives
    val columnsInChild = children.output
    val selectedAttr = columnsInChild.head

    // Creating new condition with original condition and new attribute chosen
    // TODO: parameter options
    val newCondition = RewriteConditons(condition, selectedAttr)

    // validating annotations for original condition as well as an alternative
    val filterAttrName = FILTER_OP + opNum.toString()
    val filterOrigAttrName = filterAttrName + "_Orig"
    var projExprs = columnsInChild :+
                        Alias(condition, filterOrigAttrName)() :+
                          Alias(newCondition, filterAttrName)()
//                      buildAnnotation(children, condition, filterOrigAttrName) :+
//                        buildAnnotation(children, newCondition, filterAttrName)

    val projOp = Project(projExprs, children)

    // evaluating lost by checking (candidate AND false)
    val lostByFilter = filterAttrName + "_lost"
    val lostByOrigFilter = filterOrigAttrName + "_lost"

    var lostCondOrig: Expression = null
    var lostCondNewFilter: Expression = null

    for (attr <- projExprs) {
      if (attr.name.equals("candidate")) {
        lostCondOrig = attr
        lostCondNewFilter = attr
      }

      if (attr.name.equals(filterOrigAttrName)) {
        lostCondOrig = And(lostCondOrig, Not(attr))
      }

      if (attr.name.equals(filterAttrName)) {
        lostCondNewFilter = And(lostCondNewFilter, Not(attr))
      }
    }

    projExprs = projOp.output :+
                  Alias(lostCondOrig, lostByOrigFilter)() :+
                    Alias(lostCondNewFilter, lostByFilter)()

    val projOpWithLost = Project(projExprs, projOp)

    // returning results that are candidates OR all valid
    var newCond: Expression = null

    for (attr <- projExprs) {
      if (attr.name.equals("candidate")) {
        newCond = attr
      }

      if (!columnsInChild.contains(attr)) {
        newCond = Or(newCond, attr)
      }
    }

    Filter(
      newCond,
      projOpWithLost
    )
  }

//  protected def buildAnnotation(
//                                 plan: LogicalPlan,
//                                 expr: Expression,
//                                 annotAttr: String
//                               ): NamedExpression = {
//
//    val columns = plan.output.map(_.name)
//    // TODO: conjunctive and/or disjunctive conditions
//    Alias(expr, annotAttr)()
//  }

//  Encoder information
}
