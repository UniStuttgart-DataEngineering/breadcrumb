package de.uni_stuttgart.ipvs.provenance.why_not_question

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class SchemaMatcherSuite extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer{

  def getValidatedPCTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_key = twig.createNode("flat_key")
    val nested_obj = twig.createNode("nested_obj")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(root, nested_obj, false)
    twig.validate.get
  }

  def getValidatedADTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_key = twig.createNode("flat_key")
    val nested_obj = twig.createNode("nested_obj")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(root, nested_obj, true)
    twig.validate.get
  }

  def getSingleMatchADTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a", condition = "c a")
    val b = twig.createNode("b")
    val c = twig.createNode("c")
    val f = twig.createNode("f")
    val g = twig.createNode("g", condition = "c g")
    twig = twig.createEdge(a, b)
    twig = twig.createEdge(a, c, true)
    twig = twig.createEdge(c, f, true)
    twig = twig.createEdge(c, g, true)
    twig.validate().get
  }

  def getMultipleMatchesADTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val c = twig.createNode("c")
    val f = twig.createNode("f")
    val g = twig.createNode("g")
    twig = twig.createEdge(a, c, true)
    twig = twig.createEdge(c, f, true)
    twig = twig.createEdge(c, g, true)
    twig.validate().get
  }

  def getAmbiguousADTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val c = twig.createNode("c")
    val g = twig.createNode("g")
    twig = twig.createEdge(a, c, true)
    twig = twig.createEdge(c, g, true)
    twig.validate().get
  }

  def getAmbiguousADTwig2(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val c = twig.createNode("c")
    val f = twig.createNode("f")
    val g = twig.createNode("g")
    twig = twig.createEdge(a, c, true)
    twig = twig.createEdge(c, g, true)
    twig = twig.createEdge(c, f, true)
    twig.validate().get
  }

  def getMultiplicityTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_list = twig.createNode("flat_list", min = 1, max = 5)
    val element = twig.createNode("element", condition = "contains_1")
    twig = twig.createEdge(root, flat_list, false)
    twig = twig.createEdge(flat_list, element, false)
    twig.validate.get
  }

  def getSimplePathAmbiguousTreePattern1() : Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val b = twig.createNode("b")
    val d = twig.createNode("d")
    twig = twig.createEdge(a, b, true)
    twig = twig.createEdge(b, d, false)
    twig.validate.get
  }

  def getSimplePathAmbiguousTreePattern2() : Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val b = twig.createNode("b")
    val d = twig.createNode("d")
    twig = twig.createEdge(a, b, false)
    twig = twig.createEdge(b, d, true)
    twig.validate.get
  }





  /*
  root
  |-- a: struct (nullable = true)
  |    |-- a: struct (nullable = true)
  |    |    |-- c: struct (nullable = true)
  |    |    |    |-- d: struct (nullable = true)
  |    |    |    |    |-- f: long (nullable = true)
  |    |    |    |-- e: struct (nullable = true)
  |    |    |    |    |-- a: struct (nullable = true)
  |    |    |    |    |    |-- g: long (nullable = true)
  |    |-- b: long (nullable = true)
  */

  test("PC leaf to root validation - all candidates") {
    val df = getDataFrame()
    val twig = getValidatedPCTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val candidates = schemaMatcher.findLeaveCandidates()
    assert(candidates.size == 2)
    for (candidate <- candidates) {
      candidate._1.name match {
        case "nested_obj" => {
          assert(candidate._2.size == 2)
          assert(candidate._2.contains("0.3") && candidate._2.contains("0.3.0"))
        }
        case "flat_key" => {
          assert(candidate._2.size == 1)
          assert(candidate._2(0) == "0.0")
        }
      }
    }
  }

  test("AD leaf to root validation - all candidates") {
    val df = getDataFrame()
    val twig = getValidatedADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val candidates = schemaMatcher.findLeaveCandidates()
    assert(candidates.size == 2)
    for (candidate <- candidates) {
      candidate._1.name match {
        case "nested_obj" => {
          assert(candidate._2.size == 2)
          assert(candidate._2.contains("0.3"))
          assert(candidate._2.contains("0.3.0"))
        }
        case "flat_key" => {
          assert(candidate._2.size == 1)
          assert(candidate._2(0) == "0.0")
        }
      }
    }
  }

  test("CommonAncestor") {
    val df = getDataFrame()
    val twig = getValidatedADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)

    var common = schemaMatcher.getCommonAncestorsLabel("0.3.7", "0.3.0")
    assert(common == "0.3")

    common = schemaMatcher.getCommonAncestorsLabel("0.3", "0.3.0")
    assert(common == "0.3")
  }


  test("Check Candidates") {
    val df = getDataFrame()
    val twig = getValidatedADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val matches = schemaMatcher.getCandidates()
    assert(matches.length == 2)

  }


  test("Test Cross Join") {
    val df = getDataFrame()
    val twig = getValidatedADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val x = "a" :: "b" :: "c" :: Nil
    val y = "1" :: "2" :: "3" :: Nil
    val z = Seq.empty[String]
    val all = x :: y :: z :: Nil
    val res = schemaMatcher.crossJoin(all)
    assert(res.isEmpty)
  }

  test("PC Path creation") {
    val df = getDataFrame()
    val twig = getValidatedPCTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    assert(res.size == 1)
  }

  test("AD Path creation") {
    val df = getDataFrame()
    val twig = getValidatedADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    assert(res.size == 2)
  }

  test("AD check single candidate - DOC1") {
    val df = getDataFrame(pathToDoc1)
    val twig = getSingleMatchADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    assert(res.size == 1)
  }

  test("AD check multiple candidates - DOC1") {
    val df = getDataFrame(pathToDoc1)
    val twig = getMultipleMatchesADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    assert(res.size == 2)
  }

  test("AD check single candidate - DOC2") {
    val df = getDataFrame(pathToDoc2)
    val twig = getSingleMatchADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    assert(res.size == 1)
  }

  test("AD check multiple candidates - DOC2") {
    val df = getDataFrame(pathToDoc2)
    val twig = getMultipleMatchesADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    assert(res.size == 2)
  }

  test("AD check ambiguous candidates on Path - DOC1") {
    val df = getDataFrame(pathToDoc1)
    df.printSchema()
    val twig = getAmbiguousADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()

    //TODO: Build match tree during recursion and return candidates as result in order to find all matching trees
    assert(res.size == 4)
  }

  test("AD check ambiguous candidates on BranchingNode - DOC3") {
    val df = getDataFrame(pathToDoc3)
    df.printSchema()
    val twig = getAmbiguousADTwig2()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    //TODO: Build match tree during recursion and return candidates as result in order to find all matching trees
    assert(res.size == 6)
  }

  test("Get all constraints") {
    val df = getDataFrame(pathToDoc1)
    val twig = getSingleMatchADTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    val singleMatch = res(0)
    val constraints = singleMatch.getContraints()
    assert(constraints.size == 2)
  }

  test("correct association on AD Path 1 - DOC 4") {
    val df = getDataFrame(pathToDoc4)
    val twig = getSimplePathAmbiguousTreePattern1()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)

    val res = schemaMatcher.checkCandidates()
    assert(res.size == 1)

    val singleMatch = res(0)
    assert(singleMatch.schemaMatches.filter(x => x.getTwigNode() != None).length == 3)

    val a = singleMatch.getRoot() // a
    val b1 = a.descendants.toSeq(0)
    val c = b1.descendants.toSeq(0)
    val b2 = c.descendants.toSeq(0)
    assert(b2.getName(schema) == b2.getTwigNode().get.name)
    assert(b2.getTwigNode().get.name == "b")

    val d = b2.descendants.toSeq(0)
    assert(d.getName(schema) == d.getTwigNode().get.name)
    assert(d.getTwigNode().get.name == "d")
  }


  test("correct association on AD Path 2 - DOC 4") {
    val df = getDataFrame(pathToDoc4)
    val twig = getSimplePathAmbiguousTreePattern2()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val res = schemaMatcher.checkCandidates()
    assert(res.size == 1)
    val singleMatch = res(0)
    assert(singleMatch.schemaMatches.filter(x => x.getTwigNode() != None).length == 3)

    val a = singleMatch.getRoot()
    val b1 = a.descendants.toSeq(0)
    assert(b1.getName(schema) == b1.getTwigNode().get.name)
    assert(b1.getTwigNode().get.name == "b")

    val c = b1.descendants.toSeq(0)
    val b2 = c.descendants.toSeq(0)

    val d = b2.descendants.toSeq(0)
    assert(d.getName(schema) == d.getTwigNode().get.name)
    assert(d.getTwigNode().get.name == "d")
  }

  test("Check correct associations"){
    val df = getDataFrame()
    val twig = getMultiplicityTwig()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val matches = schemaMatcher.getCandidates()
    assert(matches.size == 1)
    //_match.getLeaves().foreach(println)
    val _match = matches(0)
    _match.schemaMatches.foreach(println)

    val root = _match.getRoot()
    assert(root.getName(schema) ==  "root")
    assert(root.getTwigNode().isDefined)
    assert(root.getName(schema) ==  root.getTwigNode().get.name)
    assert(root.descendants.size == 1)

    val flat_list = root.descendants.toSeq(0)
    assert(flat_list.getName(schema) ==  "flat_list")
    assert(flat_list.getTwigNode().isDefined)
    val flTwigNode = flat_list.getTwigNode().get
    assert(flat_list.getName(schema) ==  flTwigNode.name)
    assert(flTwigNode.min == 1)
    assert(flTwigNode.max == 5)
    assert(flat_list.descendants.size == 1)


    val element = flat_list.descendants.toSeq(0)
    assert(element.getName(schema) ==  "element")
    assert(element.getTwigNode().isDefined)
    assert(element.getName(schema) ==  element.getTwigNode().get.name)

  }





}
