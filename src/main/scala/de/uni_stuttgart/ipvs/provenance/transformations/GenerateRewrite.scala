package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTrace
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, Explode, Expression, GreaterThan, IsNotNull, Literal, NamedExpression, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, LogicalPlan, Project}
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

object GenerateRewrite {
  def apply(generate: Generate, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new GenerateRewrite(generate, whyNotQuestion, oid)
}

class GenerateRewrite(generate: Generate, whyNotQuestion: SchemaSubsetTree, oid: Int) extends UnaryTransformationRewrite(generate, whyNotQuestion, oid) {

  def survivorColumnInner(provenanceContext: ProvenanceContext, flattenInputColumn: Expression): NamedExpression = {
    val attributeName = Constants.getSurvivorFieldName(oid)
    provenanceContext.addSurvivorAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(And(IsNotNull(flattenInputColumn), GreaterThan(Size(flattenInputColumn), Literal(0))), attributeName)()
  }


  override def rewrite(): Rewrite = {
    val childRewrite = WhyNotPlanRewriter.rewrite(generate.child, SchemaBackTrace(generate, whyNotQuestion).unrestructure().head)
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext

    var generateRewrite : LogicalPlan = Generate(generate.generator, generate.unrequiredChildIndex, true, generate.qualifier, generate.generatorOutput, rewrittenChild)

    if (!generate.outer) {
      generateRewrite = generate.generator match {
        case e: Explode => {
          Project(generateRewrite.output :+ survivorColumnInner(provenanceContext, e.child) :+ compatibleColumn(childRewrite.plan, childRewrite.provenanceContext), generateRewrite)
        }
        case _ => {
          throw new MatchError("Unsupported generator in Generate Expression")
        }
      }
    }
    Rewrite(generateRewrite, provenanceContext)
  }

}
