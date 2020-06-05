package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Explode, Expression, GreaterThan, IsNotNull, Literal, NamedExpression, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, LogicalPlan, Project}
import org.apache.spark.sql.types.BooleanType

object GenerateRewrite {
  def apply(generate: Generate, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new GenerateRewrite(generate, whyNotQuestion, oid)
}

class GenerateRewrite(generate: Generate, whyNotQuestion: SchemaSubsetTree, oid: Int) extends UnaryTransformationRewrite(generate, whyNotQuestion, oid) {

  override def unrestructure(): SchemaSubsetTree = {
    //TODO: ReplaceStubWithRealAggregation
    whyNotQuestion
  }

  def survivorColumnInner(provenanceContext: ProvenanceContext, flattenInputColumn: Expression): NamedExpression = {
    val attributeName = Constants.getSurvivorFieldName(oid)
    provenanceContext.addSurvivorAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(And(IsNotNull(flattenInputColumn), GreaterThan(Size(flattenInputColumn), Literal(0))), attributeName)()
  }


  //TODO: move to TransformationRewrite or provenanceContext
  def getPreviousCompatible(rewrite: Rewrite): NamedExpression = {
    val attribute = rewrite.provenanceContext.getMostRecentCompatibilityAttribute()
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in provenance structure"))
    val compatibleAttribute = rewrite.plan.output.find(ex => ex.name == attribute.attributeName)
      .getOrElse(throw new MatchError("Unable to find previous compatible structure in output of previous operator"))
    compatibleAttribute
  }

  //TODO: Add revalidation of compatibles here, i.e. replace this stub with a proper implementation
  def compatibleColumn(rewrite: Rewrite): NamedExpression = {
    val lastCompatibleAttribute = getPreviousCompatible(rewrite)
    val attributeName = Constants.getCompatibleFieldName(oid)
    rewrite.provenanceContext.addCompatibilityAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(lastCompatibleAttribute, attributeName)()
  }

  override def rewrite(): Rewrite = {
    val childRewrite = WhyNotPlanRewriter.rewrite(generate.child, unrestructure())
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext



    var generateRewrite : LogicalPlan = Generate(generate.generator, generate.unrequiredChildIndex, true, generate.qualifier, generate.generatorOutput, rewrittenChild)

    if (!generate.outer) {
      generateRewrite = generate.generator match {
        case e: Explode => {
          Project(generateRewrite.output :+ survivorColumnInner(provenanceContext, e.child) :+ compatibleColumn(childRewrite), generateRewrite)
        }
        case _ => {
          throw new MatchError("Unsupported generator in Generate Expression")
        }
      }
    }
    Rewrite(generateRewrite, provenanceContext)
  }




}
