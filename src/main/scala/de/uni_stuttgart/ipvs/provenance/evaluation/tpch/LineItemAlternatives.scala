package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}

object LineItemAlternatives {
  def apply() = new LineItemAlternatives()
}

class LineItemAlternatives extends TPCHAlternatives {

  implicit class Crossable(xs: List[Seq[String]]) {
    def cross(ys: List[Seq[String]]) = for { x <- xs; y <- ys } yield (x.toList ++ y.toList)
  }

  def createAlternatives1(primarySchemaSubsetTree: PrimarySchemaSubsetTree, attributeAlternativeSet1: Seq[String]): PrimarySchemaSubsetTree = {
    val altPerms = attributeAlternativeSet1.permutations.toList
    val totalAlternatives = altPerms.size - 1
    val primaryTree = createAlternatives(primarySchemaSubsetTree, totalAlternatives)
    for ((tree, combination) <- primaryTree.alternatives zip altPerms.tail) {
      createAlternative(tree, attributeAlternativeSet1, combination)
    }
    primaryTree
  }

  def createAlternatives2(primarySchemaSubsetTree: PrimarySchemaSubsetTree, attributeAlternativeSet1: Seq[String], attributeAlternativeSet2: Seq[String] ): PrimarySchemaSubsetTree = {
    val altPerms1 = attributeAlternativeSet1.permutations.toList
    val altPerms2 = attributeAlternativeSet2.permutations.toList
    val totalAlternatives = altPerms1.size * altPerms2.size - 1
    val allPerms : List[List[String]] = altPerms1 cross altPerms2
    val original : List[String] = allPerms.head
    val primaryTree = createAlternatives(primarySchemaSubsetTree, totalAlternatives)
    for ((tree, combination) <- primaryTree.alternatives zip allPerms.tail) {
      createAlternative(tree, original, combination)
    }
    primaryTree
  }

  def createAlternatives3(primarySchemaSubsetTree: PrimarySchemaSubsetTree, attributeAlternativeSet1: Seq[String], attributeAlternativeSet2: Seq[String]): PrimarySchemaSubsetTree = {
    val altPerms1 = attributeAlternativeSet1.permutations.toList
    val altPerms2 = attributeAlternativeSet2.map { x => List(x)}.toList
    val totalAlternatives = altPerms1.size * altPerms2.size - 1
    val allPerms : List[List[String]] = altPerms1 cross altPerms2
    val original : List[String] = allPerms.head
    val primaryTree = createAlternatives(primarySchemaSubsetTree, totalAlternatives)
    for ((tree, combination) <- primaryTree.alternatives zip allPerms.tail) {
      createAlternative(tree, original, combination)
    }
    primaryTree
  }

  def createAlternative(tree: SchemaSubsetTree, originalSet: Seq[String], attributeAlternativeSet: Seq[String]): Unit = {
    for ((original, alternative) <- originalSet zip attributeAlternativeSet) {
      if (original != alternative) {
        replaceWithTMP(tree.rootNode, original)
      }
    }
    for ((original, alternative) <- originalSet zip attributeAlternativeSet) {
      if (original != alternative) {
        replaceTMPWithAlternative(tree.rootNode, original, alternative)
      }
    }
  }

  override def createAllAlternatives(primarySchemaSubsetTree: PrimarySchemaSubsetTree): PrimarySchemaSubsetTree = createAlternatives2(primarySchemaSubsetTree, Seq("l_discount", "l_tax"), Seq("l_shipdate", "l_receiptdate", "l_commitdate"))
}
