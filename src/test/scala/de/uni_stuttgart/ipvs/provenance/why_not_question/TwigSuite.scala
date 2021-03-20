package de.uni_stuttgart.ipvs.provenance.why_not_question

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import org.scalatest.FunSuite

class TwigSuite extends FunSuite {

  def getValidatedTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    val nested_obj = twig.createNode("nested_obj", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(root, nested_obj, false)
    twig.validate.get
  }

  test("TestTwigCreation") {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    assert(twig.isValidated() == false)
    assert(twig.nodes.contains(root))
    assert(twig.nodes.contains(flat_key))
    assert(twig.edges.size == 1)
  }

  test("TestTwigValidation") {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.validate().getOrElse(new Twig())
    assert(twig.isValidated())
  }

  test("TestTwigValidation - Invalid Twig - No Edge") {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig.validate()
    assert(twig.isValidated() == false)
  }

  test("TestTwigValidation - Invalid Twig - Circle with root") {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(flat_key, root, false)
    twig = twig.validate().getOrElse(new Twig())
    assert(twig.isValidated() == false)
  }

  test("TestTwigValidation - Invalid Twig - Circle without root") {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    val nested_obj = twig.createNode("nested_obj", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(root, nested_obj, false)
    val thrown = intercept[RuntimeException]{
      twig.createEdge(flat_key, nested_obj, false)
    }
    assert(thrown.isInstanceOf[RuntimeException])
  }

  test("TestTwigValidation - Invalid Twig - two roots") {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.validate().getOrElse(new Twig())
    assert(twig.isValidated() == false)
  }

  test("getRoot"){
    val twig = getValidatedTwig()
    val root = twig.getRoot()
    assert(root.name == "root")
  }

  test("getLeaves"){
    val twig = getValidatedTwig()
    val leaves = twig.getLeaves()
    assert(leaves.size == 2)
    val names = leaves.map(leaf => leaf.name)
    assert(names.contains("flat_key"))
    assert(names.contains("nested_obj"))
    assert(!names.contains("root"))
  }

  test("getBranching"){
    val twig = getValidatedTwig()
    val branching = twig.getBranches()
    assert(branching.size == 1)
    val names = branching.map(branching => branching.name)
    assert(names.contains("root"))
  }

  def getMultipleMatchesADTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val b = twig.createNode("b")
    val c = twig.createNode("c")
    val f = twig.createNode("f")
    val g = twig.createNode("g")
    twig = twig.createEdge(a, b)
    twig = twig.createEdge(a, c, true)
    twig = twig.createEdge(c, f, true)
    twig = twig.createEdge(c, g, true)
    twig.validate().get
  }

  test("indexing"){
    val twig = getMultipleMatchesADTwig()
    twig.indexBranchingNodes()
    val branches = twig.getBranches()
    for (branchingNode <- branches){
      branchingNode.name match {
        case "a" => {assert(branchingNode.branchingIndex == 2)}
        case "c" => {assert(branchingNode.branchingIndex == 1)}
      }
    }
  }
}
