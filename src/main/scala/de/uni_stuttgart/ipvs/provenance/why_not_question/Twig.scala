package de.uni_stuttgart.ipvs.provenance.why_not_question

import scala.collection.mutable.ListBuffer


class TwigNode (val name : String, val min: Int, val max: Int, val condition: String) {

  var ancestorEdge : Option[TwigEdge] = None
  val descendantEdges = scala.collection.mutable.Set.empty[TwigEdge]
  var branchingIndex = 0

  def setAncestor(ancestor: TwigEdge): Unit = {
    if (ancestorEdge.isDefined) {
      throw new RuntimeException("Try to set ancestor twice")
    } else {
      ancestorEdge = Some(ancestor)
    }
  }

  def setBranchingIndex(idx: Int) : Unit = {
    branchingIndex = idx
  }

  def addDescendant(descendant: TwigEdge): Unit = {
    descendantEdges.add(descendant)
  }

  def isLeaf() : Boolean = {
    descendantEdges.isEmpty
  }

  def isBranching() : Boolean = {
    descendantEdges.size  > 1
  }

  def hasConstraint() : Boolean = {
    hasCondition() || hasMultiplicity()
  }

  def hasCondition() : Boolean = {
    condition.nonEmpty
  }

  def hasMultiplicity() : Boolean = {
    max > 0
  }

  def isRoot() : Boolean = {
    ancestorEdge.isEmpty
  }

  def getDescendants(): Seq[TwigNode] = {
    descendantEdges.toSeq.map(desc => desc.descendant)
  }

  def getAncestor(): Option[TwigNode] = {
    if (ancestorEdge.isDefined) {
      Some(ancestorEdge.get.ancestor)
    } else {
      None
    }
  }

  def isFlexibleBranching() : Boolean = {
    isBranching() && descendantEdges.forall(edge => edge.adRelationship)
  }

  def getBranchingORLeafDescendants() : Seq[TwigNode] = {
    getDescendants().map(child => getBranchingOrLeafDescendant(child))
  }

  def getBranchingOrLeafDescendant(twigNode: TwigNode): TwigNode = {
    //when a node is neither branching nor a leaf it has exactly one child
    if (twigNode.isBranchingOrLeaf()) twigNode else getBranchingOrLeafDescendant(twigNode.getDescendants()(0))
  }

  def isBranchingOrLeaf(): Boolean = {
    isBranching() || isLeaf()
  }


  def getAncestorAndType(): Option[(TwigNode, Boolean)] = {
    if (ancestorEdge.isDefined) {
      Some(ancestorEdge.get.ancestor, ancestorEdge.get.adRelationship)
    } else {
      None
    }
  }

  def getDepth(): Int = {
    var depth = 0
    var aEdge = this.ancestorEdge
    while (aEdge.isDefined) {
      depth += 1
      aEdge = aEdge.get.ancestor.ancestorEdge
    }
    depth
  }

  def getJSONObject: String = {
    val childs = ListBuffer[String]()
    for (child <- this.getDescendants()) {
      childs.append(child.getJSONObject)
    }

    s"""{
        "name": "$name",
        "condition": "$condition",
        "min": $min,
        "max": $max,
        "adRelation": ${if (isRoot()) false else ancestorEdge.get.adRelationship},
        "children": ${childs.mkString("[", ", ", "]")}
      }""".stripMargin.replaceAll("\\s{2,}", "")
  }

}

class TwigEdge (val ancestor: TwigNode, val descendant: TwigNode, val adRelationship: Boolean) {

  ancestor.addDescendant(this)
  descendant.setAncestor(this)

}

class Twig() {
  var nodes = scala.collection.mutable.Set.empty[TwigNode]
  var edges = scala.collection.mutable.Set.empty[TwigEdge]
  private var validated = false

  def createNode(name : String,min: Int =  1, max: Int = -1, condition: String = "") : TwigNode = {
    val newTwig = new TwigNode(name, min, max, condition)
    nodes += newTwig
    newTwig
  }

  def createEdge(ancestor: TwigNode, descendant: TwigNode, adRelationship: Boolean = false): Twig = {
    nodes += ancestor
    nodes += descendant
    val edge = new TwigEdge(ancestor, descendant, adRelationship)
    edges += edge
    this
  }

  def isValidated() : Boolean = {
    validated
  }

  def validate() : Option[Twig] = {
    val roots = nodes.filter(n => n.isRoot())
    if (roots.size != 1) {return None}
    val root = roots.toSeq(0)
    val nodesToBeChecked = nodes.clone()


    val res = followChildren(root, nodesToBeChecked, true)
    if (res._2){
      this.validated = true
      Some(this)
    } else {
      None
    }
  }

  def getLeaves():scala.collection.mutable.Set[TwigNode] = {
    if (validated){
      return nodes.filter(node => node.isLeaf())
    }
    throw new RuntimeException("Tree is not validated, call validate() on the tree first.")
  }

  def getBranches():scala.collection.mutable.Set[TwigNode] = {
    if (validated){
      return nodes.filter(node => node.isBranching())
    }
    throw new RuntimeException("Tree is not validated, call validate() on the tree first.")
  }

  def getBranches(level: Int):scala.collection.mutable.Set[TwigNode] = {
    return getBranches().filter(node => node.branchingIndex == level)
  }

  def getRoot(): TwigNode = {
    if (validated){
      return nodes.filter(node => node.isRoot()).toSeq(0)
    }
    throw new RuntimeException("Tree is not validated, call validate() on the tree first.")
  }

  private def followChildren(twigNode: TwigNode, nodesToBeChecked : scala.collection.mutable.Set[TwigNode], valid : Boolean) : (scala.collection.mutable.Set[TwigNode], Boolean) = {
    var remainingNodes = nodesToBeChecked - twigNode
    val sizeDiff = nodesToBeChecked.size - remainingNodes.size
    var stillValid = valid && (sizeDiff == 1)
    for (child <- twigNode.getDescendants()){
      val result = followChildren(child, remainingNodes, stillValid)
      remainingNodes = result._1
      stillValid = stillValid && result._2
    }
    if (stillValid && twigNode.min != -1 && twigNode.max != -1) {
      // min and max set, check that min <= max
      stillValid &= twigNode.min <= twigNode.max
    }
    (remainingNodes, stillValid)
  }

  def indexBranchingNodes(): Unit = {
    if (validated){
      indexBranchingNode(getRoot())
    }
  }

  private def indexBranchingNode(node: TwigNode): Int = {
    if (node.isBranching()){
      var idx = 0
      for (child <- node.getDescendants()){
        idx = math.max(idx, indexBranchingNode(child))
      }
      idx += 1
      node.setBranchingIndex(idx)
      return idx
    }
    return 0
  }

  def toJSON: String = {
    if (validated) {
      this.getRoot().getJSONObject
    } else {
      "{\"error\": \"Tree is not validated, call validate() on the tree first and make sure it\'s valid.\"}"
    }
  }
}

object Twig {
  def apply() = new Twig()
}