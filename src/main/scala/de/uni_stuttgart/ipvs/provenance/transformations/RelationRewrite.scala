package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaNode, PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree, SchemaSubsetTreeBackTracing}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Project}


object RelationRewrite {
  def apply(relation: LeafNode, oid: Int)  = new RelationRewrite(relation, oid)
}

class RelationRewrite(relation: LeafNode, oid: Int) extends InputTransformationRewrite(relation, oid){


  //TODO: Stub, works only for the running example
  def findSchemaAlternatives(): PrimarySchemaSubsetTree = {
    val primarySchemaSubsetTree = PrimarySchemaSubsetTree(whyNotQuestion)
    val alternative1 = whyNotQuestion.deepCopy()
    replaceAddress(alternative1.rootNode)
    replaceValue(alternative1.rootNode)
    associateNode(primarySchemaSubsetTree.getRootNode, alternative1.rootNode)
    primarySchemaSubsetTree.alternatives += alternative1
    primarySchemaSubsetTree
  }

  //TODO: Stub, works only for the running example
  //TODO: not a generic approach, since sets are unordered
  def associateNode(primeNode: PrimarySchemaNode, alternativeNode: SchemaNode): Unit = {
    primeNode.addAlternative(alternativeNode)
    (primeNode.getChildren zip alternativeNode.children)
      .map { case (primeChild, alternativeChild) => associateNode(primeChild, alternativeChild)}
  }

  //TODO: Stub, works only for the running example
  def replaceAddress(node: SchemaNode): Unit ={
    if (node.name == "address2") {
      node.name = "address1"
      return
    }
    for (child <- node.children){
      replaceAddress(child)
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

  override def rewriteWithAlternatives(): Rewrite = {
    val provenanceContext = new ProvenanceContext()
    val alternatives = findSchemaAlternatives()
    provenanceContext.primarySchemaAlternative = alternatives
    val projectList = relation.output ++ compatibleColumns(relation, provenanceContext)
    val rewrittenLocalRelation = Project(
      projectList,
      relation
    )
    Rewrite(rewrittenLocalRelation, provenanceContext)
  }

}
