package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaAlternativesExpressionAlternatives, SchemaAlternativesForwardTracing, SchemaSubsetTree, SchemaSubsetTreeBackTracing}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, CaseWhen, ElementAt, EqualTo, Explode, Expression, GreaterThan, Greatest, IsNotNull, LessThanOrEqual, Literal, NamedExpression, Or, PosExplode, ScalaUDF, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  def createMaxColumn(alternativeInputExpressions: Seq[Expression]): NamedExpression = {
    Alias(Greatest(alternativeInputExpressions.map(exp => Size(exp)) :+ Literal(1)), Constants.getFlattenMaxColName(oid))()
  }

  def planWithMaxColumn(logicalPlan: LogicalPlan, alternativeInputExpressions: Seq[Expression]): LogicalPlan = {
    val projectList = logicalPlan.output :+ createMaxColumn(alternativeInputExpressions)
    Project(projectList, logicalPlan)
  }

  def getAttribute(output: Seq[Attribute], attributeName: String): Attribute = {
    output.find(attribute => attribute.name == attributeName).get
  }

  def getFlattenIndexColumn(child: LogicalPlan) = {
    val udf = ProvenanceContext.getFlattenUDF
    val children = Seq(getAttribute(child.output, Constants.getFlattenMaxColName(oid)))
    val inputIsNullSafe = true ::  Nil
    val inputTypes = udf.inputTypes.getOrElse(Seq.empty[DataType])
    val udfName = Some(Constants.getFlattenMaxColName(oid))
    Alias(ScalaUDF(udf.f, udf.dataType, children, inputIsNullSafe, inputTypes, udfName), Constants.getFlattenArrayColName(oid))()
  }

  def planWithNestedIndexColumn(logicalPlan: LogicalPlan): LogicalPlan = {
    val projectList = logicalPlan.output :+ getFlattenIndexColumn(logicalPlan)
    Project(projectList, logicalPlan)
  }

  def planWithFlattenedIndexColumn(logicalPlan: LogicalPlan): LogicalPlan = {
    val nestedIndexColumn = getAttribute(logicalPlan.output, Constants.getFlattenArrayColName(oid))
    val explode = Explode(nestedIndexColumn)
    val outputAttribute = AttributeReference(Constants.getFlattenIdxColName(oid), IntegerType)()
    Generate(explode, Nil, false, Some(Constants.getFlattenIdxColName(oid)), Seq(outputAttribute), logicalPlan)
  }

  def getFlattenedColumns(input: Expression, output: NamedExpression, index: Expression): NamedExpression ={
    val condition: Expression = LessThanOrEqual(index, Size(input))
    val trueCase: Expression = ElementAt(input, index)
    val falseCase: Expression = Literal(null, output.dataType)
    val caseExpression = CaseWhen(Seq((condition, trueCase)), falseCase)
    caseExpression.resolved
    Alias(caseExpression, output.name)()
  }

  def getAllAlternativeIds(primarySchemaSubsetTree: PrimarySchemaSubsetTree): Seq[Int] = {
    primarySchemaSubsetTree.getAllAlternatives() map {
      alt => alt.id
    }
  }

  def planWithFlattenedColumns(logicalPlan: LogicalPlan, alternativeInputExpressions: Seq[Expression], alternativeOutputExpressions: Seq[NamedExpression]) : LogicalPlan = {
    val indexColumn = getAttribute(logicalPlan.output, Constants.getFlattenIdxColName(oid))
    val arrayAccessExpressions: Seq[NamedExpression] = (alternativeInputExpressions zip alternativeOutputExpressions) map {
      case (input, output) => getFlattenedColumns(input, output, indexColumn)
      //case (input, output) => Alias(ElementAt(input, indexColumn), output.name)()
    }

    val projectList = logicalPlan.output ++ arrayAccessExpressions
    Project(projectList, logicalPlan)
  }

  def getFlattenedAttributes(attributes: Seq[AttributeReference], names: Seq[String]) : Seq[Attribute] = {
    names.map(name => getAttribute(attributes, name))
  }

  def getIndexAttribute(attributes: Seq[Attribute]) : Attribute = {
    getAttribute(attributes, Constants.getFlattenIdxColName(oid))
  }

  def getValidColumn(alternativeId: Int, input: Expression, index: Attribute, oldValidColumn: Attribute): NamedExpression ={
    val condition: Expression = Or(LessThanOrEqual(index, Size(input)), And(EqualTo(index, Literal(1, IntegerType)), EqualTo(Size(input), Literal(0, IntegerType))))
    val trueCase: Expression = oldValidColumn
    val falseCase: Expression = Literal(false, BooleanType)
    val caseExpression = CaseWhen(Seq((condition, trueCase)), falseCase)
    caseExpression.resolved
    Alias(caseExpression, Constants.getValidFieldName(alternativeId))()
  }

  def getSurvivorColumn(alternativeId: Int, input: Expression, index: Attribute): NamedExpression ={
    val condition: Expression = LessThanOrEqual(index, Size(input))
    val trueCase: Expression = Literal(true, BooleanType)
    val falseCase: Expression = Literal(false, BooleanType)
    val caseExpression = CaseWhen(Seq((condition, trueCase)), falseCase)
    caseExpression.resolved
    Alias(caseExpression, Constants.getSurvivorFieldName(oid, alternativeId))()
  }

  def planWithRenamedValidColumns(plan: LogicalPlan, provenanceContext: ProvenanceContext): (LogicalPlan, Seq[String]) = {
    val provenanceAttributes = provenanceContext.getValidAttributes()
    val oldValidColumns = getAttributesByName(plan.output, provenanceAttributes.map(attr => attr.attributeName))
    val remainingAttributes = plan.output.filterNot(attribute => oldValidColumns.contains(attribute))
    val renamedAttributes = renameValidColumns(oldValidColumns)
    val projectList = remainingAttributes ++ renamedAttributes
    val newPlan = Project(projectList, plan)
    (newPlan, renamedAttributes.map(attr => attr.name))
  }

  def getValidColumns(logicalPlan: LogicalPlan, provenanceContext: ProvenanceContext, alternativeInputExpressions: Seq[Expression], oldValidColumns: Seq[Attribute]): Seq[NamedExpression] = {
    val alternativeIds = getAllAlternativeIds(provenanceContext.primarySchemaAlternative)
    val index = getIndexAttribute(logicalPlan.output)
    val validColumns = ((alternativeInputExpressions zip alternativeIds) zip oldValidColumns) map {
      case ((alternativeInputExpression, alternativeId), oldValidColumn) => {
        getValidColumn(alternativeId, alternativeInputExpression, index, oldValidColumn)
      }
    }
    val provenanceAttributes = validColumns.map(validColumn => ProvenanceAttribute(oid, validColumn.name, validColumn.dataType))
    provenanceContext.replaceValidAttributes(provenanceAttributes)
    validColumns
  }

  def getSurvivorColumns(logicalPlan: LogicalPlan, provenanceContext: ProvenanceContext, alternativeInputExpressions: Seq[Expression]): Seq[NamedExpression] = {
    val alternativeIds = getAllAlternativeIds(provenanceContext.primarySchemaAlternative)
    val index = getIndexAttribute(logicalPlan.output)
    val survivorColumns = (alternativeInputExpressions zip alternativeIds) map {
      case (alternativeInputExpression, alternativeId) => {
        getSurvivorColumn(alternativeId, alternativeInputExpression, index)
      }
    }
    val provenanceAttributes = survivorColumns.map(survivorColumn => ProvenanceAttribute(oid, survivorColumn.name, survivorColumn.dataType))
    provenanceContext.addSurvivorAttributes(provenanceAttributes)
    survivorColumns
  }

  def planWithProvenanceColumns(logicalPlan: LogicalPlan, provenanceContext: ProvenanceContext, alternativeInputExpressions: Seq[Expression], oldValidColumns: Seq[Attribute]): Project = {
    val compatibles = compatibleColumns(logicalPlan, provenanceContext)
    val survivors = getSurvivorColumns(logicalPlan, provenanceContext, alternativeInputExpressions)
    val valids = getValidColumns(logicalPlan, provenanceContext, alternativeInputExpressions, oldValidColumns)
    Project(logicalPlan.output ++ compatibles ++ survivors ++ valids, logicalPlan)
  }


  override def rewriteWithAlternatives(): Rewrite = {
    val childRewrite = child.rewriteWithAlternatives()
    val rewrittenChild = childRewrite.plan
    val provenanceContext = childRewrite.provenanceContext
    val (alternativeInputs, alternativeOutputs) = SchemaAlternativesExpressionAlternatives(provenanceContext.primarySchemaAlternative, rewrittenChild, generate.output).forwardTraceGenerator(generate.generator.children, generate.generatorOutput)
    val updatedTree = SchemaAlternativesForwardTracing(provenanceContext.primarySchemaAlternative, rewrittenChild, generate.output).forwardTraceGenerator(generate.generator.children, generate.generatorOutput)
    provenanceContext.primarySchemaAlternative = updatedTree

    //val generateRewrite : LogicalPlan = Generate(posExplode, generate.unrequiredChildIndex, true, generate.qualifier, output.toList, rewrittenChild)
    val withMaxCol = planWithMaxColumn(rewrittenChild, alternativeInputs)
    val withNestedIndex = planWithNestedIndexColumn(withMaxCol)
    val withFlattenedIndex = planWithFlattenedIndexColumn(withNestedIndex)
    val withFlattenedColumns = planWithFlattenedColumns(withFlattenedIndex, alternativeInputs, alternativeOutputs)
    val (withOldValidColumns, oldValidColumnNames) = planWithRenamedValidColumns(withFlattenedColumns, provenanceContext)
    val oldValidColumns = getAttributesByName(withOldValidColumns.output, oldValidColumnNames)
    val withProvenanceColumns = planWithProvenanceColumns(withOldValidColumns, provenanceContext, alternativeInputs, oldValidColumns)


    Rewrite(withProvenanceColumns, provenanceContext)
  }

}
