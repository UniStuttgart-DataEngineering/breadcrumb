package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.why_not_question.{MatchNode, Schema, SchemaMatch}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.typedLit

import scala.collection.mutable.ListBuffer

object SchemaSubsetTree {
  private var currentId = 0
  protected def getId(): Int = {
    currentId += 1
    currentId
  }

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

  def apply(withRootNode: Boolean = true, id: Int = -1) = {
    val schemaSubTree = new SchemaSubsetTree(id)
    if (withRootNode) {
      schemaSubTree.rootNode = SchemaNode("root")
    }
    schemaSubTree
  }
}

class SchemaSubsetTree(_id: Int = -1) {

  val id = if (_id < 0) SchemaSubsetTree.getId() else _id

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

  def serializeReducedTree(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    resetMarking(rootNode)
    markNodesWithConditions(rootNode)
    val nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    serializeNodeWithCondition(rootNode, nodes, 0, 0.toShort)
    nodes.toList
  }

  def resetMarking(node: SchemaNode): Unit = {
    node.inPathWithCondition = false
    for (child <- node.children){
      resetMarking(child)
    }
  }

  def markNodesWithConditions(node: SchemaNode): Unit = {
    if (node.children.isEmpty){
      if (node.hasValueConstraint()) {
        node.inPathWithCondition = true
      }
    } else {
      for (child <- node.children){
        markNodesWithConditions(child)
        node.inPathWithCondition |= child.inPathWithCondition
      }
    }

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

  def serializeNodeWithCondition(node: SchemaNode, nodes: ListBuffer[(Short, Short, Byte, Int, Int, String, String)], id: Short, parentId: Short): Short = {
    if (!node.inPathWithCondition){
      return id
    }
    nodes += node.serialize(id, parentId)
    var currentId : Short = id
    for (child <- node.children){
      if (child.inPathWithCondition) {
        currentId = (currentId + 1).toShort
        currentId = serializeNodeWithCondition(child, nodes, currentId, id)
      }
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
    typedLit(serializeReducedTree()).expr
  }


}
