package de.uni_stuttgart.ipvs.provenance.why_not_question

import scala.collection.mutable.ListBuffer

object SchemaMatch {

  var uid = 0L
  def apply(root: TwigNode, rootLabel: String, schema: Schema) = new SchemaMatch(root, rootLabel, schema, {uid += 1; uid})

}

class SchemaMatch(root: TwigNode, rootLabel: String, schema: Schema, id: Long) {

  val uid = id

  val rootNode = new MatchNode(rootLabel, Some(root), 0)
  var schemaMatches = rootNode :: Nil

  private[provenance] def addPath(leaf : TwigNode, label: String): Unit = {
    val leafMatch = recursivelyAddPath(rootNode, schema.getTrailingNodeIDs(rootNode.label, label))
    leafMatch.setAssociatedTwigNode(leaf)
  }

  private def recursivelyAddPath(currentNode: MatchNode, remainingNodeLabels: Seq[String]): MatchNode = {
    if (remainingNodeLabels.isEmpty) {return currentNode}
    val childLabel = currentNode.label + "." + remainingNodeLabels(0)
    val candidate = currentNode.descendants.find(matchNode => matchNode.label == childLabel)
    candidate match { //returns leaf node
      case Some(x) => {recursivelyAddPath(x, remainingNodeLabels.drop(1))}
      case None => {
        recursivelyAddPath(createMatchNode(currentNode, childLabel, currentNode.depth + 1), remainingNodeLabels.drop(1))
      }
    }
  }

  private def createMatchNode(currentNode: MatchNode, childLabel: String, depth: Int) = {
    val node = new MatchNode(childLabel, None, depth)
    node.ancestor = Some(currentNode)
    schemaMatches = schemaMatches :+ node
    currentNode.descendants += node
    node
  }

  def getLeaves() : Seq[MatchNode] = {
    schemaMatches.filter(x => x.isLeaf())
  }

  def getMatchNode(twigNode: TwigNode): Option[MatchNode] ={
    schemaMatches.find(node => {
      node.getTwigNode() match {
        case Some(t)=> t == twigNode
        case None => false
      }
    })
  }

  def getCommonAncestor(descendants: Seq[TwigNode]): Option[MatchNode] = {
    val descendantMatchNodes = descendants.map(twigNode => getMatchNode(twigNode).getOrElse(rootNode))
    val labelPrefix = schema.getCommonLabelPrefix(descendantMatchNodes.map(x => x.label))
    schemaMatches.find(x => x.label == labelPrefix)
  }

  def getRoot(): MatchNode = {
    rootNode
  }

  def getAllPaths(): Seq[String] = {
    schemaMatches.map(node => node.getSparkAccessPath(schema)).filterNot(x => x == "")
  }


  def getContraints(): Seq[MatchConstraint] = {
    val constraints = ListBuffer.empty[MatchConstraint]
    for (node <- schemaMatches){
      val optionNode = node.getTwigNode()
      val twigNode = optionNode match {
        case Some(x) => if (x.hasConstraint) constraints.append(new MatchConstraint(node.getSparkAccessPath(schema), x.min, x.max, x.condition, node.isNestedAttribute(schema)))
        case None => {}
      }
    }
    constraints.toList
  }


  def serialize(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    val nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    val currentId = serializePathToRootNode(nodes)
    serializeNode(rootNode, nodes, currentId, Math.max(currentId-1, 0).toShort)
    nodes.toList
  }

  // side-effect: Fill list buffer; increase Id
  private[provenance] def serializePathToRootNode(nodes: ListBuffer[(Short, Short, Byte, Int, Int, String, String)]): Short = {
    val path = rootNode.getSchemaPath(schema).dropRight(1) // ignore the schema element itself
    var currentId : Short = 0
    for (element <- path){
      val node = (Math.max(currentId-1, 0).toShort, currentId, 0.toByte, 1, -1, element, "")
      nodes += node
      currentId = (currentId + 1).toShort
    }
    currentId
  }

  // side-effect: Fill list buffer; increase Id
  def serializeNode(node: MatchNode, nodes: ListBuffer[(Short, Short, Byte, Int, Int, String, String)], id: Short, parentId: Short): Short = {
    nodes += node.serialize(id, parentId, schema)
    var currentId : Short = id
    for (child <- node.descendants){
      currentId = (currentId + 1).toShort
      currentId = serializeNode(child, nodes, currentId, id)
    }
    currentId // Check whether id labeling works properly
  }




}

@Deprecated
class MatchConstraint(_matchPath: String, _min: Long, _max: Long, _condition: String, _isNestedCollectionConstraint: Boolean) {
  val matchPath = _matchPath
  val min = if (_isNestedCollectionConstraint && _min == 0) 1 else _min
  val max = if (_isNestedCollectionConstraint && _max == 0) Long.MaxValue else _max
  val condition = _condition
  val isNestedCollectionConstraint = _isNestedCollectionConstraint

}

class MatchNode(schemaLabel : String, twigNode: Option[TwigNode], _depth: Int) {


  var ancestor : Option[MatchNode] = None
  val descendants = scala.collection.mutable.Set.empty[MatchNode]
  var associatedTwigNode = twigNode
  val label = schemaLabel
  val depth = _depth

  private[provenance] def setAssociatedTwigNode(twigNode: TwigNode): Unit ={
    associatedTwigNode = Some(twigNode)
  }

  private[provenance] def resetAssociatedTwigNode(): Unit = {
    associatedTwigNode = None
  }

  def isLeaf(): Boolean ={
    descendants.isEmpty
  }

  def getTwigNode(): Option[TwigNode] = {
    associatedTwigNode
  }

  def getName(schema: Schema): String ={
    schema.getName(label).getOrElse("")
  }

  @deprecated
  def getSparkAccessPath(schema: Schema): String ={
    schema.getSparkAccessPath(label).getOrElse("")
  }

  def getSchemaPath(schema: Schema): Seq[String] = {
    schema.getSchemaPath(label).getOrElse(Seq.empty[String])
  }

  def isNestedAttribute(schema: Schema): Boolean = {
    schema.isNested(schemaLabel)
  }

  /*
def parseRow(row: Row) : UDFTreeNode = {
  id = row.getAs[Short](0) // id
  parentId = row.getAs[Short](1)
  comparisonOp = row.getAs[Byte](2) // comparison_operator
  min = row.getAs[Int](3) // min
  max = row.getAs[Int](4) // max
  attributeName = row.getAs[String](5) // attribute_name
  attributeValue = row.getAs[String](6) // attribute_value
  this
}*/

  private def getPrefix(condition: String):String = {
    condition.substring(0, Math.min(8,condition.length))
  }

  private def getOperatorId(condition: String): Byte = {
    getPrefix(condition) match {
      //TODO remove hack for contains statement
      case "contains" => 2
      case "ltltltlt" => 3
      case "gtgtgtgt" => 4
      case "nconncon" => 5
      case "lengthgt" => 6
      case "lengthlt" => 7
      case x => if (x.length() > 0) 1 else 0
    }
  }

  private def getAttributeValue(condition: String, operatorId: Byte): String = {
    if (operatorId < 2) condition else condition.substring(Math.min(8, condition.length-1))
  }

  def serialize(id: Short, parentId: Short, schema: Schema):(Short, Short, Byte, Int, Int, String, String) = {


    var min = 1
    var max = -1 // ignore until set
    var attributeName = ""

    var condition = ""
    var comparisonOp: Byte = 0
    var attributeValue = ""

    attributeName=getName(schema)

    getTwigNode() match {
      case Some(tNode) => {min = tNode.min; max = tNode.max; condition=tNode.condition}
      case None => {}
    }

    comparisonOp = getOperatorId(condition)
    attributeValue = getAttributeValue(condition, comparisonOp)
    (id, parentId, comparisonOp, min, max, attributeName, attributeValue)
  }

}