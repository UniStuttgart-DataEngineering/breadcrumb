package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaMatch, TwigNode}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

class SchemaSubsetTreeTest extends FunSuite with SharedSparkTestDataFrames with MockFactory{

  //1) Deep copy
  //2) From SchemaMatch

  test("[Constructor] from SchemaMatch one node"){

    val schema = new Schema(null, true)
    val testName = "testNode"
    schema.labeles.put("0", testName)
    schema.initializeNameStreams(schema.labeles)
    val twigNode = new TwigNode(testName, 2, 3, "gtgtgtgt5")
    val schemaMatch = SchemaMatch(twigNode: TwigNode, "0", schema)
    val schemaSubsetTree = SchemaSubsetTree(schemaMatch, schema)
    assert(schemaSubsetTree.rootNode.name == testName)
    assert(schemaSubsetTree.rootNode.constraint.min == 2)
  }

  test("[Constructor] from SchemaMatch two nodes"){
    val schema = new Schema(null, true)
    val testName = "testNode"
    val testChild = "child"
    schema.labeles.put("0", testName)
    schema.labeles.put("0.1", testChild)
    schema.initializeNameStreams(schema.labeles)
    val twigParent = new TwigNode(testName, 2, 3, "gtgtgtgt5")
    val twigChild = new TwigNode(testChild, 7, 8, "gtgtgtgt9")
    val schemaMatch = SchemaMatch(twigParent: TwigNode, "0", schema)
    schemaMatch.addPath(twigChild, "0.1")
    val schemaSubsetTree = SchemaSubsetTree(schemaMatch, schema)
    assert(schemaSubsetTree.rootNode.name == testName)
    assert(schemaSubsetTree.rootNode.children.head.name == testChild)
    assert(schemaSubsetTree.rootNode.children.head.parent == schemaSubsetTree.rootNode)
  }

}
