package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite, WhyNotPlanRewriter}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaNode, SchemaSubsetTree}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, Explode, Expression, GreaterThan, IsNotNull, Literal, NamedExpression, Size}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, LogicalPlan, Project}
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

object GenerateRewrite {
  def apply(generate: Generate, whyNotQuestion:SchemaSubsetTree, oid: Int)  = new GenerateRewrite(generate, whyNotQuestion, oid)
}

class GenerateRewrite(generate: Generate, whyNotQuestion: SchemaSubsetTree, oid: Int) extends UnaryTransformationRewrite(generate, whyNotQuestion, oid) {

  var unrestructuredWhyNotQuestionInput: SchemaSubsetTree = null
  var tempTree: SchemaSubsetTree = whyNotQuestion.deepCopy()

  def getAllStructFields(st: StructType, newNode: SchemaNode): SchemaNode = {
    for (eachStruct <- st) {
      val structNode = SchemaNode("")

      if (eachStruct.dataType.typeName.equals("struct")) {
        getAllStructFields(eachStruct.dataType.asInstanceOf[StructType], structNode)
      }

      structNode.name = eachStruct.name
      val node = tempTree.getNodeByName(structNode.name)
      if (node != null) structNode.constraint = node.constraint.deepCopy()

      newNode.addChild(structNode)
      structNode.setParent(newNode)
    }

    newNode
  }


  def unrestructureAttributeRefForUnnesting(ar: AttributeReference, newRoot: SchemaNode, newGenerate: Generate): SchemaNode = {
    val node = whyNotQuestion.getNodeByName(ar.name)
    val parent = newGenerate.generator.asInstanceOf[Explode]

    // Recursively unrestructure if multiple collections exist
    if (newGenerate.child.isInstanceOf[Project]) {
      val projectOp = newGenerate.child.asInstanceOf[Project]
      val generateOp = projectOp.child.asInstanceOf[Generate]
      val innerAttrRef = generateOp.generatorOutput.head.asInstanceOf[AttributeReference]
      unrestructureAttributeRefForUnnesting(innerAttrRef, newRoot, generateOp)
    }

    val parentAsAttrRef = parent.child.asInstanceOf[AttributeReference]
    val newNode: SchemaNode = SchemaNode(parentAsAttrRef.name)

    if (node != null) {
      val newChildNode: SchemaNode = SchemaNode("")
      newChildNode.copyNode(node)
      newChildNode.name = "element"

      tempTree.rootNode.copyNode(newChildNode)
      newChildNode.children.clear()

      if (ar.dataType.typeName.equals("struct")) {
        val fields = ar.dataType.asInstanceOf[StructType]
        var newFields = Array[StructField]()

        for (f <- fields) {
          val childName = node.children.find(node => node.name == f.name).getOrElse("")
          if (!childName.equals("")) newFields = newFields :+ f

//          //TODO: matching to input schema
//          newFields = newFields :+ f
        }

        getAllStructFields(StructType(newFields), newChildNode)
      }

      // Set element node
      newNode.addChild(newChildNode)
      newChildNode.setParent(newNode)

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

    for (ex <- generate.generatorOutput) {
      ex match {
        case ar: AttributeReference => {
          unrestructureAttributeRefForUnnesting(ar, newRoot, generate)
        }
        case _ => // do nothing
      }
    }

    unrestructuredWhyNotQuestionInput
  }

  def survivorColumnInner(provenanceContext: ProvenanceContext, flattenInputColumn: Expression): NamedExpression = {
    val attributeName = Constants.getSurvivorFieldName(oid)
    provenanceContext.addSurvivorAttribute(ProvenanceAttribute(oid, attributeName, BooleanType))
    Alias(And(IsNotNull(flattenInputColumn), GreaterThan(Size(flattenInputColumn), Literal(0))), attributeName)()
  }









  //TODO: Add revalidation of compatibles here, i.e. replace this stub with a proper implementation
  def compatibleColumn(rewrite: Rewrite): NamedExpression = {
    val lastCompatibleAttribute = getPreviousCompatible(rewrite)
    val attributeName = addCompatibleAttributeToProvenanceContext(rewrite.provenanceContext)
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
