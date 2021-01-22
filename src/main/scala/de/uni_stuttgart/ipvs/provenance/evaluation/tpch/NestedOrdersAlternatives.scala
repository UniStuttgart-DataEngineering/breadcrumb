package de.uni_stuttgart.ipvs.provenance.evaluation.tpch
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.PrimarySchemaSubsetTree

object NestedOrdersAlternatives extends LineItemAlternatives {
  //override def createAllAlternatives(primarySchemaSubsetTree: PrimarySchemaSubsetTree): PrimarySchemaSubsetTree = primarySchemaSubsetTree

  def createAlternativesWithOrdersWith1Permutations(primarySchemaSubsetTree: PrimarySchemaSubsetTree, attributeAlternativeSetOrder: Seq[String], attributeAlternativeSet1: Seq[String], attributeAlternativeSet2: Seq[String]): PrimarySchemaSubsetTree = {
    val altPerms0 = attributeAlternativeSetOrder.permutations.toList
    val altPerms1 = attributeAlternativeSet1.permutations.toList
    val altPerms2 = attributeAlternativeSet2.map { x => List(x)}.toList
    val totalAlternatives = altPerms0.size * altPerms1.size * altPerms2.size - 1
    val allPerms : List[List[String]] = altPerms0 cross altPerms1 cross altPerms2
    val original : List[String] = allPerms.head
    val primaryTree = createAlternatives(primarySchemaSubsetTree, totalAlternatives)
    for ((tree, combination) <- primaryTree.alternatives zip allPerms.tail) {
      createAlternative(tree, original.drop(2), combination.drop(2))
      if (original.head != combination.head) {
        OrdersAlternatives.replaceAlternative(tree.rootNode)
      }
    }
    primaryTree
  }

  def createAlternativesWithOrdersWith2Permutations(primarySchemaSubsetTree: PrimarySchemaSubsetTree, attributeAlternativeSetOrder: Seq[String], attributeAlternativeSet1: Seq[String], attributeAlternativeSet2: Seq[String]): PrimarySchemaSubsetTree = {
    val altPerms0 = attributeAlternativeSetOrder.permutations.toList
    val altPerms1 = attributeAlternativeSet1.permutations.toList
    val altPerms2 = attributeAlternativeSet2.combinations(2).flatMap(x => x.permutations.toList).toList
    val totalAlternatives = altPerms0.size * altPerms1.size * altPerms2.size - 1
    val allPerms : List[List[String]] = altPerms0 cross altPerms1 cross altPerms2
    val original : List[String] = allPerms.head
    val primaryTree = createAlternatives(primarySchemaSubsetTree, totalAlternatives)
    for ((tree, combination) <- primaryTree.alternatives zip allPerms.tail) {
      createAlternative(tree, original.drop(2), combination.drop(2))
      if (original.head != combination.head) {
        OrdersAlternatives.replaceAlternative(tree.rootNode)
      }
    }
    primaryTree
  }
}
