package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.nested_why_not.{ProvenanceContext, Rewrite}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaSubsetTree, SchemaSubsetTreeModifications}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Project}


object RelationRewrite {
  def apply(relation: LeafNode, oid: Int)  = new RelationRewrite(relation, oid)
}

class RelationRewrite(relation: LeafNode, oid: Int) extends InputTransformationRewrite(relation, oid){




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

}
