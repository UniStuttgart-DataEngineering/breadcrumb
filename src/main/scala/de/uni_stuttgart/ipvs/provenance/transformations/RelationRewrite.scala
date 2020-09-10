package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceAttribute, ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaNode, PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree, SchemaSubsetTreeBackTracing}
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Project}
import org.apache.spark.sql.types.BooleanType


object RelationRewrite {
  def apply(relation: LeafNode, oid: Int)  = new RelationRewrite(relation, oid)
}

class RelationRewrite(relation: LeafNode, oid: Int) extends InputTransformationRewrite(relation, oid){


  //TODO: Stub, works only for the running example
  def findSchemaAlternatives(): PrimarySchemaSubsetTree = {
    if (ProvenanceContext.testScenario != null){
      return ProvenanceContext.testScenario.computeAlternatives(whyNotQuestion, this.plan)
    }
    val primarySchemaSubsetTree = PrimarySchemaSubsetTree(whyNotQuestion)
    val alternative1 = createAlternative(primarySchemaSubsetTree)
    replaceAddress1(alternative1.rootNode)
    replaceAddress2(alternative1.rootNode)
    replaceTempAddress(alternative1.rootNode)
    replaceValue(alternative1.rootNode)
    replaceJKey(alternative1.rootNode)
    replaceNestedObj(alternative1.rootNode)
    //associateNode(primarySchemaSubsetTree.getRootNode, alternative1.rootNode)
    //primarySchemaSubsetTree.alternatives += alternative1
    primarySchemaSubsetTree
  }

  def createAlternative(primarySchemaSubsetTree: PrimarySchemaSubsetTree): SchemaSubsetTree = {
    val alternative = SchemaSubsetTree()
    primarySchemaSubsetTree.addAlternative(alternative)
    primarySchemaSubsetTree.getRootNode.addAlternative(alternative.rootNode)
    for (child <- primarySchemaSubsetTree.getRootNode.getChildren) {
      child.createDuplicates(100, primarySchemaSubsetTree.getRootNode, false, false)
    }
    alternative
  }

  //TODO: Stub, works only for the running example
  //TODO: not a generic approach, since sets are unordered
  def associateNode(primeNode: PrimarySchemaNode, alternativeNode: SchemaNode): Unit = {
    primeNode.addAlternative(alternativeNode)
    (primeNode.getChildren zip alternativeNode.children)
      .map { case (primeChild, alternativeChild) => associateNode(primeChild, alternativeChild)}
  }

  //TODO: Stub, works only for the running example
  def replaceAddress2(node: SchemaNode): Unit ={
    if (node.name == "address2") {
      node.name = "address1"
      return
    }
    for (child <- node.children){
      replaceAddress2(child)
    }
  }

  def replaceAddress1(node: SchemaNode): Unit ={
    if (node.name == "address1") {
      node.name = "temp_address"
      return
    }
    for (child <- node.children){
      replaceAddress1(child)
    }
  }

  def replaceTempAddress(node: SchemaNode): Unit ={
    if (node.name == "temp_address") {
      node.name = "address2"
      return
    }
    for (child <- node.children){
      replaceTempAddress(child)
    }
  }



  //TODO: Stub, works only for the test
  def replaceValue(node: SchemaNode): Unit ={
    if (node.name == "value") {
      node.name = "otherValue"
      return
    }
    for (child <- node.children){
      replaceValue(child)
    }
  }

  //TODO: Stub, works only for the test
  def replaceJKey(node: SchemaNode): Unit ={
    if (node.name == "jkey") {
      node.name = "okey"
      return
    }
    for (child <- node.children){
      replaceJKey(child)
    }
  }

  def replaceNestedObj(node: SchemaNode): Unit ={
    if (node.name == "nested_obj") {
      node.name = "nested_obj_alt"
    } else if (node.name == "nested_obj_1") {
      node.name = "nested_obj_alt_1"
    }
    for (child <- node.children){
      replaceNestedObj(child)
    }
  }


  override def rewrite: Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val projectList = relation.output :+ compatibleColumn(relation, provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )
    Rewrite(rewrittenLocalRelation, provenanceContext)
  }

  override protected[provenance] def undoSchemaModifications(schemaSubsetTree: SchemaSubsetTree): SchemaSubsetTree = {
    schemaSubsetTree.deepCopy()
//    SchemaSubsetTreeModifications(schemaSubsetTree, Nil, relation.output, relation.expressions).getInputTree()
  }

  def validColumn(alternativeId: Int): NamedExpression = {
    Alias(Literal(true, BooleanType), Constants.getValidFieldName(alternativeId))()
  }

  def validColumns(provenanceContext: ProvenanceContext): Seq[NamedExpression] ={
    val columns = provenanceContext.primarySchemaAlternative.getAllAlternatives().map(alternative => validColumn(alternative.id))
    provenanceContext.replaceValidAttributes(columns.map(col => ProvenanceAttribute(oid, col.name, BooleanType)))
    columns
  }

  def originalColumn(alternativeId: Int): NamedExpression = {
    Alias(Literal(true, BooleanType), Constants.getOriginalFieldName(alternativeId))()
  }

  def originalColumns(provenanceContext: ProvenanceContext): Seq[NamedExpression] ={
    val columns = provenanceContext.primarySchemaAlternative.getAllAlternatives().map(alternative => originalColumn(alternative.id))
    provenanceContext.replaceOriginalAttributes(columns.map(col => ProvenanceAttribute(oid, col.name, BooleanType)))
    columns
  }

  override def rewriteWithAlternatives(): Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val alternatives = findSchemaAlternatives()
    provenanceContext.primarySchemaAlternative = alternatives
    val projectList = relation.output ++ compatibleColumns(relation, provenanceContext) ++ validColumns(provenanceContext) ++ originalColumns(provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )
    Rewrite(rewrittenLocalRelation, provenanceContext)
  }

}
