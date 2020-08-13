package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaSubsetTree, SchemaSubsetTreeBackTracing}
import de.uni_stuttgart.ipvs.provenance.why_not_question.SchemaBackTrace
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Explode, Expression, GreaterThan, IsNotNull, Literal, NamedExpression, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project}
import org.apache.spark.sql.types.BooleanType

object GenerateRewrite {
  def apply(generate: Generate, oid: Int)  = new GenerateRewrite(generate, oid)
}

class GenerateRewrite(generate: Generate, oid: Int) extends UnaryTransformationRewrite(generate, oid) {

  def survivorColumnInner(provenanceContext: ProvenanceContext, flattenInputColumn: Expression): NamedExpression = {
    val attributeName = Constants.getSurvivorFieldName(oid)
    provenanceContext.addSurvivorAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(And(IsNotNull(flattenInputColumn), GreaterThan(Size(flattenInputColumn), Literal(0))), attributeName)()
  }


  override def rewrite(): Rewrite = {
    //val childRewrite = WhyNotPlanRewriter.rewrite(generate.child, SchemaBackTrace(generate, whyNotQuestion).unrestructure().head)
    val childRewrite = child.rewrite()
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext

    var generateRewrite : LogicalPlan = Generate(generate.generator, generate.unrequiredChildIndex, true, generate.qualifier, generate.generatorOutput, rewrittenChild)

    if (!generate.outer) {
      generateRewrite = generate.generator match {
        case e: Explode => {
          Project(generateRewrite.output :+ survivorColumnInner(provenanceContext, e.child) :+ compatibleColumn(generateRewrite, childRewrite.provenanceContext), generateRewrite)
        }
        case _ => {
          throw new MatchError("Unsupported generator in Generate Expression")
        }
      }
    }
    Rewrite(generateRewrite, provenanceContext)
  }

  override protected[provenance] def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    //TODO: Are these assumptions well-chosen?
    assert(generate.generator.children.size == 1)
    assert(generate.generatorOutput.size == 1)

    SchemaSubsetTreeBackTracing(schemaSubsetTree, generate.child.output, generate.generatorOutput, generate.generator.children).backtraceGenerator()

//    val modifications = SchemaSubsetTreeModifications(schemaSubsetTree, generate.child.output, generate.generatorOutput, generate.generator.children)
//    if (whyNotQuestion.rootNode.children.size != schemaSubsetTree.rootNode.children.size)
//      modifications.setInitialInputTree(whyNotQuestion.deepCopy())
//    modifications.backtraceGenerator()
//    modifications.getInputTree()
//    //SchemaBackTrace(generate, whyNotQuestion).unrestructure().head
  }

  override def rewriteWithAlternatives(): Rewrite = rewrite()

}
