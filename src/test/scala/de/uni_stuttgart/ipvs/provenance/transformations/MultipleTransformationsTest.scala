package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.explode
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




}
