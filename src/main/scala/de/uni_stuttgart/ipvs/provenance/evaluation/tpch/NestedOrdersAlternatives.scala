package de.uni_stuttgart.ipvs.provenance.evaluation.tpch
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.PrimarySchemaSubsetTree

object NestedOrdersAlternatives extends LineItemAlternatives {
  //override def createAllAlternatives(primarySchemaSubsetTree: PrimarySchemaSubsetTree): PrimarySchemaSubsetTree = primarySchemaSubsetTree

  def createAlternativesWithOrdersWith1Permutations(primarySchemaSubsetTree: PrimarySchemaSubsetTree, attributeAlternativeSetOrder: Seq[String], attributeAlternativeSet1: Seq[String], attributeAlternativeSet2: Seq[String]): PrimarySchemaSubsetTree = {
    val altPerms0 = attributeAlternativeSetOrder.permutations.toList
    val altPerms1 = attributeAlternativeSet1.permutations.toList
    val altPerms2 = attributeAlternativeSet2.map { x => List(x)}.toList
    val totalAlternativesWithoutOrders = altPerms1.size * altPerms2.size
    val totalAlternatives = 2*totalAlternativesWithoutOrders - 1
    val allPerms : List[List[String]] = altPerms1 cross altPerms2
    val original : List[String] = allPerms.head
    val primaryTree = createAlternatives(primarySchemaSubsetTree, totalAlternatives)
    for ((tree, combination) <- primaryTree.alternatives zip allPerms.tail) {
      createAlternative(tree, original, combination)
    }
    primaryTree
  }


}
