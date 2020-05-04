package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.why_not_question.{MatchNode, Schema, TwigNode}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite


class SchemaNodeTest extends FunSuite with SharedSparkTestDataFrames with MockFactory {

  test("[Constructor] without parent") {
    val name = "test"
    val res = SchemaNode(name)
    assert(res.name == name)
  }

  test("[Constructor] with parent") {
    val parentName = "parent"
    val childName = "child"
    val parent = SchemaNode(parentName)
    val child = SchemaNode(childName, parent = parent)
    assert(child.parent == parent)
  }

  test("[Constructor] with child") {
    val parentName = "parent"
    val childName = "child"
    val parent = SchemaNode(parentName)
    val child = SchemaNode(childName, parent = parent)
    parent.addChild(child)
    assert(parent.children.head == child)
  }

  test("[Constructor] constraint") {
    val constraint = Constraint("gtgtgtgt5", 2, 3)
    assert(constraint.attributeValue == "5")
    assert(constraint.operatorId == 4)
    assert(constraint.min == 2)
    assert(constraint.max == 3)
  }

  test("[Constructor] with constraint") {
    val constraint = Constraint("gtgtgtgt5", 2, 3)
    val name = "test"
    val res = SchemaNode(name, constraint)
    assert(res.constraint.min == 2)
  }



  test("[Constructor] from SchemaNode"){
    val name = "node_name"
    val schema = new Schema(null, true)
    val matchNode = stub[MatchNode]
    (matchNode.getName _).when(schema).returns(name)
    (matchNode.getTwigNode _).when().returns(None)
    val res = SchemaNode(matchNode, schema, null)
    matchNode.getTwigNode()
    assert(res.name == name)
  }

  test("[Constructor] from SchemaNode with constraint"){
    val name = "node_name"
    val schema = new Schema(null, true)
    val twigNode = new TwigNode("twig_node", 2, 3, "gtgtgtgt5")
    val matchNode = stub[MatchNode]
    (matchNode.getName _).when(schema).returns(name)
    (matchNode.getTwigNode _).when().returns(Some(twigNode))
    val res = SchemaNode(matchNode, schema, null)
    assert(res.name == name)
    assert(res.constraint.attributeValue == "5")
    assert(res.constraint.operatorId == 4)
    assert(res.constraint.min == 2)
    assert(res.constraint.max == 3)
  }

  test("[Serialization]"){
    val constraint = Constraint("gtgtgtgt5", 2, 3)
    val name = "test"
    val toBeSerialized = SchemaNode(name, constraint)
    val res = toBeSerialized.serialize(1, 0)
    assert(res == (1, 0, 4, 2, 3, "test", "5"))
  }

  test("[Deep Copy]") {
    val parentName = "parent"
    val childName = "child"
    val constraint = Constraint("gtgtgtgt5", 2, 3)
    val parent = SchemaNode(parentName)
    val child = SchemaNode(childName, constraint, parent)
    parent.addChild(child)
    val res = parent.deepCopy(null)
    assert(res != parent)
    assert(res.children.head != null)
    assert(res.children.head != child)
    assert(res.children.head.constraint == child.constraint) // case classes implement equals method
  }









}


