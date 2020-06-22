package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.why_not_question.{MatchNode, Schema, SchemaMatch}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.typedLit

import scala.collection.mutable.ListBuffer

object SchemaSubsetTree {
  private def fromSchemaMatch(schemaMatch: SchemaMatch, schema: Schema): SchemaSubsetTree = {
    val schemaSubsetTree = new SchemaSubsetTree()
    val root = fromSchemaMatchRecursive(schemaMatch.getRoot(), schema, null)
    schemaSubsetTree.rootNode = root
    schemaSubsetTree
  }

  private def fromSchemaMatchRecursive(matchNode: MatchNode, schema: Schema, parent: SchemaNode): SchemaNode = {
    val node = SchemaNode(matchNode, schema, parent)
    for (child <- matchNode.descendants){
      node.addChild(fromSchemaMatchRecursive(child, schema, node))
    }
    node
  }

  def apply(schemaMatch: SchemaMatch,schema: Schema) = fromSchemaMatch(schemaMatch, schema: Schema)
  def apply(withRootNode: Boolean = true) = {
    val schemaSubTree = new SchemaSubsetTree
    if (withRootNode) {
      schemaSubTree.rootNode = SchemaNode("root")
    }
    schemaSubTree
  }
}

class SchemaSubsetTree {

  var rootNode: SchemaNode = null

  def deepCopy(): SchemaSubsetTree = {
    val res = new SchemaSubsetTree()
    res.rootNode = rootNode.deepCopy(null)
    res
  }

  def serialize(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    val nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    serializeNode(rootNode, nodes, 0, 0.toShort)
    nodes.toList
  }

  // side-effect: Fill list buffer; increase Id
  def serializeNode(node: SchemaNode, nodes: ListBuffer[(Short, Short, Byte, Int, Int, String, String)], id: Short, parentId: Short): Short = {
    nodes += node.serialize(id, parentId)
    var currentId : Short = id
    for (child <- node.children){
      currentId = (currentId + 1).toShort
      currentId = serializeNode(child, nodes, currentId, id)
    }
    currentId // Check whether id labeling works properly
  }

  def getNodeByPath(path: Seq[String]): Option[SchemaNode] ={
    var currentNode = rootNode
    for (nodeName <- path){
      currentNode = currentNode.getChild(nodeName).getOrElse(return None)
    }
    Some(currentNode)
  }

  def getNodeByName(name: String): SchemaNode ={
    var currentNode = rootNode
    currentNode = currentNode.getChild(name).getOrElse(null)
    currentNode
  }

  def addRenamedCopyOfNodeToParent(nodeToAdd: SchemaNode, parent:SchemaNode = rootNode): SchemaNode = {
    nodeToAdd.deepCopy(parent)
  }

  def moveNodeToNewParentByPath(nodePath: Seq[String], newParentPath: Seq[String]): Option[SchemaNode] = {
    val node = getNodeByPath(nodePath).getOrElse(return None)
    var newParent = rootNode
    if (newParentPath != null && newParentPath.nonEmpty){
      newParent = getNodeByPath(newParentPath).getOrElse(return None)
    }
    moveNodeToNewParent(node, newParent)
    Some(node)
  }


  def moveNodeToNewParent(node: SchemaNode, newParent: SchemaNode): Unit = {
    if (node.parent != null) {
      node.parent.children -= node
    }
    node.setParent(newParent)
    newParent.children += node
  }
  def getSchemaSubsetTreeExpression : Expression = {
    typedLit(serialize()).expr
  }


}
