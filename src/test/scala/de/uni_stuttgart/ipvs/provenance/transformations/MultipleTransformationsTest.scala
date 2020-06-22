package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.functions.{explode, struct}
import org.scalatest.FunSuite

class MultipleTransformationsTest extends FunSuite with SharedSparkTestDataFrames {

  import spark.implicits._

  def nestedWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val nested_list = twig.createNode("nested_list", 1, 1, "")
    val element1 = twig.createNode("element", 1, 1, "1_list_val_1_1")
    twig = twig.createEdge(root, nested_list, false)
    twig = twig.createEdge(nested_list, element1, false)
    twig.validate().get
  }

  test("[Rewrite] Explode contains survived field") {
    val df = getDataFrame(pathToNestedData00)
    var otherDf = df.withColumn("flattened", explode($"nested_list"))
    otherDf = otherDf.filter($"flat_key" === "1_flat_val_x")
    otherDf = otherDf.select($"nested_list")
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    res.explain(true)
    res.show()
  }


  def whyNotTupleProjectionNewName(newName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode(newName, 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }


  test("[Unrestructure] Filter over Project (Attribute is renamed)") {
    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"flat_key".alias(newName))
    res = res.filter($"renamed" === "1_flat_val_x")

    val schemaMatch = getSchemaMatch(res, whyNotTupleProjectionNewName(newName))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter],schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = ProjectRewrite(plan.children.head.asInstanceOf[Project],rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)
    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "flat_key")
    assert(rewrittenSchemaSubset1.rootNode.children.head.name != rewrittenSchemaSubset2.rootNode.children.head.name)
  }


  test("[Unrestructure] Project over Filter (Attribute is renamed)") {
    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.filter($"flat_key" === "1_flat_val_x")
    res = res.select($"flat_key".alias(newName))

    val schemaMatch = getSchemaMatch(res, whyNotTupleProjectionNewName(newName))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = ProjectRewrite(plan.asInstanceOf[Project],schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = FilterRewrite(plan.children.head.asInstanceOf[Filter],rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)
    assert(rewrittenSchemaSubset1.rootNode.children.head.name == "flat_key")
    assert(rewrittenSchemaSubset1.rootNode.children.head.name == rewrittenSchemaSubset2.rootNode.children.head.name)
  }


  test("[Unrestructure] Filter over nested tuple") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"obj")
    res = res.filter($"obj.obj_sub.key" === 1)

    val schemaMatch = getSchemaMatch(res, whyNotTupleProjectionNewName("obj"))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter],schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = ProjectRewrite(plan.children.head.asInstanceOf[Project],rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "obj")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "obj_sub")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.children.head.name == "key")
  }


  test("[Unrestructure] Filter over renamed nested tuple") {
    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"obj".alias(newName))
    res = res.filter($"renamed.obj_sub.key" === 1)

    val schemaMatch = getSchemaMatch(res, whyNotTupleProjectionNewName(newName))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter],schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = ProjectRewrite(plan.children.head.asInstanceOf[Project],rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "obj")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "obj_sub")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.children.head.name == "key")
  }


  test("[Unrestructure] Filter over nested tuple with multiple columns") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData)
    var res = df.select($"user")
    res = res.filter($"user.name" === "Lisa Paul")

    val schemaMatch = getSchemaMatch(res, whyNotTupleProjectionNewName("user"))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter],schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = ProjectRewrite(plan.children.head.asInstanceOf[Project],rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    val id = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
    val userName = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "user")
    assert(id.name == "id_str")
    assert(userName.name == "name")
  }


  def whyNotTupleStruct(newName: String, newSubName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val nested_obj1 = twig.createNode(newName, 1, 1, "")
    val nested_obj2 = twig.createNode(newSubName, 1, 1, "")
    //    val nested_key = twig.createNode("key")
    twig = twig.createEdge(root, nested_obj1, false)
    twig = twig.createEdge(nested_obj1, nested_obj2, false)
    //    twig = twig.createEdge(nested_obj2, nested_key, false)
    twig.validate().get
  }


  test("[Unrestructure] Filter over nested tuple 2") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"obj")
    res = res.filter($"obj.obj_sub.key" === 1)

    val schemaMatch = getSchemaMatch(res, whyNotTupleStruct("obj", "obj_sub"))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter],schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = ProjectRewrite(plan.children.head.asInstanceOf[Project],rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "obj")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "obj_sub")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.children.head.name == "key")
  }



  test("[Unrestructure] Filter over nested tuple with multiple columns, but reference one attribute") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData)
    var res = df.select($"user")
    res = res.filter($"user.name" === "Lisa Paul")

    val schemaMatch = getSchemaMatch(res, whyNotTupleStruct("user", "id_str"))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter],schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = ProjectRewrite(plan.children.head.asInstanceOf[Project],rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "user")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "id_str")

    val size = rewrittenSchemaSubset2.rootNode.children.size
    assert(size == 1)
  }



}
