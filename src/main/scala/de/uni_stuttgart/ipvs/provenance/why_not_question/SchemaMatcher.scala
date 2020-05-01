package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer


object SchemaMatcher {

  def apply(twig: Twig, schema: Schema) = new SchemaMatcher(twig, schema)

}


class SchemaMatcher(twig: Twig, schema: Schema) {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[provenance] def findLeaveCandidates(): scala.collection.mutable.Map[TwigNode, Seq[String]] = {
    val leaves = twig.getLeaves()
    val allCandidates = scala.collection.mutable.Map.empty[TwigNode, Seq[String]]
    for (leaf <- leaves) {
      allCandidates.put(leaf, findLeaveCandidates(leaf))
    }
    allCandidates
  }

  private[provenance] def findLeaveCandidates(leaf : TwigNode) : Seq[String] = {
    schema.getNameStream(leaf.name)
    //schema.getNameStream(leaf.name).filter(label => validCandidate(leaf, label))
  }

  private[provenance] def findRootCandidates() : Seq[String] = {
    val root = twig.getRoot()
    schema.getNameStream(root.name)
  }

  private[provenance] def getCommonAncestorsLabel(leftLabel: String, rightLabel: String): String = {
    val commonAncestorLabel = leftLabel.zip(rightLabel).takeWhile(Function.tupled(_ == _)).map(_._1).mkString
    commonAncestorLabel match {
      case x if x endsWith "." => x.dropRight(1)
      case x => x
    }
  }

  private[provenance] def rootIsAncestorOfLeaf(rootLabel: String, leafLabel: String): Boolean = {
    getCommonAncestorsLabel(rootLabel, leafLabel) == rootLabel
  }

  def getCandidates(): Seq[SchemaMatch] = {
    checkCandidates()
  }

  def getSerializedMatches():Seq[Seq[(Short, Short, Byte, Int, Int, String, String)]] = {
    getCandidates().map(m => m.serialize())
  }

  private[provenance] def checkCandidates(): Seq[SchemaMatch] = {
    val candidates = ListBuffer.empty[SchemaMatch]
    for (root <- findRootCandidates()) {
      val potentialMatchingLeaves = findMatchingLeaves(root)
      var resultList = Seq.empty[Seq[(TwigNode, String)]]
      for ((node, labelList) <- potentialMatchingLeaves) {
        val leafNodesAndLabels = labelList.map(label => (node, label))
        resultList = resultList :+ leafNodesAndLabels
      }

      val crossed = crossJoin(
        resultList
      )

      //crossed.foreach(println)
      for (candidate <- crossed) {
        //TODO: This needs to be fixed in case we want to track multiple elements with different values in a single nested collection
        //TODO: If two twig nodes point to the same schema element only one of them is associated with the twig node, causing a none.get exception when the other is checked
        if (candidate.map(x => x._2).size == candidate.map(x => x._2).toSet.size) {
          checkCandidate(candidate, (twig.getRoot(), root)) match {
            case Some(x) => candidates += x
            case _ =>
          }
        }
      }
    }
    candidates.toList
  }

  private[provenance] def checkCandidate(leaves: Traversable[(TwigNode, String)], root: (TwigNode, String)): Option[SchemaMatch] = {
    val schemaMatch = SchemaMatch(root._1, root._2, schema)

    for (leaf <- leaves) {
      schemaMatch.addPath(leaf._1, leaf._2)
    }

    if (validateSchemaMatch(schemaMatch)) {
      return Some(schemaMatch)
    }
    None
  }


  private[provenance] def validateSchemaMatch(schemaMatch: SchemaMatch): Boolean = {
    val root = twig.getRoot()
    validateBranchingRootNode(schemaMatch, root)
  }

  private[provenance] def validateBranchingNode(schemaMatch: SchemaMatch, branchingNode: TwigNode): Boolean = {
    var isValid = true
    val isFlexNode = branchingNode.isFlexibleBranching()
    if (!isFlexNode) {
      isValid = validateNonFlexibleBranchingNode(schemaMatch, branchingNode)
    } else {
      isValid = validateFlexibleBranchingNode(schemaMatch, branchingNode)
    }
    isValid
  }

  private[provenance] def validateBranchingRootNode(schemaMatch: SchemaMatch, branchingNode: TwigNode): Boolean = {
    //guard, only valid if called from root node
    if (!branchingNode.isRoot()) return false

    var isValid = true
    val descendants = branchingNode.getBranchingORLeafDescendants()

    // first check branching descendant nodes
    for (descendant <- descendants.filter(t => t.isBranching())) {
      isValid &= validateBranchingNode(schemaMatch, descendant)
    }
    if(!isValid) return isValid

    val commonAncestor = schemaMatch.getRoot()

    // then check paths from branching nodes or leaves to the current node
    for (descendant <- descendants) {
      val descendantMatchNode = schemaMatch.getMatchNode(descendant).get
      isValid &= validatePathFromDescendantToBranchingNode(commonAncestor, descendantMatchNode)
    }
    isValid
  }

  private[provenance] def validateNonFlexibleBranchingNode(schemaMatch: SchemaMatch, branchingNode: TwigNode): Boolean = {
    var isValid = true
    val descendants = branchingNode.getBranchingORLeafDescendants()

    // first check branching nodes
    for (descendant <- descendants.filter(t => t.isBranching())) {
      isValid &= validateBranchingNode(schemaMatch, descendant)
    }

    val commonAncestorOption = schemaMatch.getCommonAncestor(descendants)
    val res = commonAncestorOption match {
      case Some(x) => schema.getName(x.label).getOrElse("") == branchingNode.name
      case None => false
    }
    isValid &= res
    if (!isValid) return false
    val commonAncestor = commonAncestorOption.get //must be defined, otherwise isValid would be false
    commonAncestor.setAssociatedTwigNode(branchingNode)

    // then check paths from branching nodes or leaves to the current node
    for (descendant <- descendants) {
      val descendantMatchNode = schemaMatch.getMatchNode(descendant).get
      isValid &= validatePathFromDescendantToBranchingNode(commonAncestor, descendantMatchNode)
    }
    isValid
  }


  private[provenance] def validateFlexibleBranchingNode(schemaMatch: SchemaMatch, branchingNode: TwigNode): Boolean = {
    var isValid = true
    val descendants = branchingNode.getBranchingORLeafDescendants()
    // first check descendant branching nodes
    for (descendant <- descendants.filter(t => t.isBranching())) {
      isValid &= validateBranchingNode(schemaMatch, descendant)
    }
    if (!isValid) return false
    // all subtrees are validated from here on, check paths starting at the common ancestor
    val commonAncestorOption = schemaMatch.getCommonAncestor(descendants)
    val res = commonAncestorOption match {
      case Some(x) => schema.getName(x.label).getOrElse("") == branchingNode.name
      case None => false
    }
    isValid &= res
    if (!isValid) return false
    // there is a common ancestor, thus, check for all ancestors from the common ancestor to the root
    val commonAncestor = commonAncestorOption.get
    val candidates = ListBuffer.empty[MatchNode]
    var iterativeMatchNode = commonAncestor
    while (iterativeMatchNode != schemaMatch.getRoot()) {
      if (branchingNode.name == iterativeMatchNode.getName(schema)) {
        candidates += iterativeMatchNode
      }
      iterativeMatchNode = iterativeMatchNode.ancestor.get
    }
    if (branchingNode.isRoot() && branchingNode.name == schemaMatch.getRoot().getName(schema)) {
      candidates += iterativeMatchNode
    }
    isValid = false
    var validCounter = 0
    for (candidate <- candidates) {
      var canditIsValid = true
      candidate.setAssociatedTwigNode(branchingNode)
      for (descendant <- descendants) {
        val descendantMatchNode = schemaMatch.getMatchNode(descendant).get
        canditIsValid &= validatePathFromDescendantToBranchingNode(commonAncestor, descendantMatchNode)
      }
      isValid |= canditIsValid
      if (!canditIsValid) {
        candidate.resetAssociatedTwigNode()
        //TODO add dissassiciation of nodes on the path to the candidate
        logger.warn("path nodes may not be disassociated properly for descendants of twig node:" + branchingNode.name)
      } else {
        validCounter += 1
      }
      if (validCounter > 1) {
        logger.warn("multiple valid nodes found for branching twig node:" + branchingNode.name)
      }
    }
    isValid
  }


  private[provenance] def validatePathFromDescendantToBranchingNode(branchningNode: MatchNode, descandantNode: MatchNode): Boolean = {
    val matchNode = descandantNode.ancestor.get
    val (twigNode, adRelationship) = descandantNode.getTwigNode().get.getAncestorAndType().get
    adRelationship match {
      case true => validateNodeForADRelationship(twigNode, matchNode, branchningNode)
      case false => validateNodeForPCRelationship(twigNode, matchNode, branchningNode)
    }
  }

  private[provenance] def validateNodeForPCRelationship(twigNode: TwigNode, matchNode: MatchNode, upperBound: MatchNode): Boolean ={
    if (twigNode == upperBound.getTwigNode().get) {
      return matchNode == upperBound
    }
    if (twigNode.name != matchNode.getName(schema)) return false
    val nextMatchNode = matchNode.ancestor.get
    val (nextTwigNode, adRelationship) = twigNode.getAncestorAndType().get
    val valid = adRelationship match {
      case true => validateNodeForADRelationship(nextTwigNode, nextMatchNode, upperBound)
      case false => validateNodeForPCRelationship(nextTwigNode, nextMatchNode, upperBound)
    }
    if (valid){
      matchNode.setAssociatedTwigNode(twigNode)
    }
    valid
  }

  private[provenance] def validateNodeForADRelationship(twigNode: TwigNode, matchNode: MatchNode, upperBound: MatchNode): Boolean = {
    if (twigNode == upperBound.getTwigNode().get) return twigNode.name == upperBound.getName(schema)
    var iterativeMatchNode = matchNode
    val candidates = ListBuffer.empty[MatchNode]

    while (iterativeMatchNode != upperBound) {
      if (twigNode.name == iterativeMatchNode.getName(schema)) {
        candidates += iterativeMatchNode
      }
      iterativeMatchNode = iterativeMatchNode.ancestor.get
    }
    var valid = false
    var validCounter = 0
    for (candidate <- candidates) {
      val nextMatchNode = candidate.ancestor.get
      val (nextTwigNode, adRelationship) = twigNode.getAncestorAndType().get
      val x = adRelationship match {
        case true => validateNodeForADRelationship(nextTwigNode, nextMatchNode, upperBound)
        case false => validateNodeForPCRelationship(nextTwigNode, nextMatchNode, upperBound)
      }
      if (x) {
        candidate.setAssociatedTwigNode(twigNode)
        validCounter += 1
      }
      valid |= x //ignores duplicate possible correct paths
    }
    if (validCounter > 1) {
      logger.warn("multiple valid nodes found for non-branching, non-leaf twig node:" + twigNode.name)
    }
    valid
  }

  private[provenance] def crossJoin[T](list: Traversable[Traversable[T]]): Traversable[Traversable[T]] =
    list match {
      case xs :: Nil => xs map (Traversable(_))
      case x :: xs => for {
        i <- x
        j <- crossJoin(xs)
      } yield Traversable(i) ++ j
    }

  private[provenance] def findMatchingLeaves(root: String): scala.collection.mutable.Map[TwigNode, Seq[String]] = {
    val leaves = twig.getLeaves()
    val allCandidates = scala.collection.mutable.Map.empty[TwigNode, Seq[String]]
    for (leaf <- leaves) {
      allCandidates.put(leaf, findLeaveCandidates(leaf).filter(leafLabel => rootIsAncestorOfLeaf(root, leafLabel)))
    }
    allCandidates
  }

}