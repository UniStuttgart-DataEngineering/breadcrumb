package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{explode, size}
import org.scalatest.FunSuite

class CollectionUnnestingTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer{

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
      val df = getDataFrame(pathToNestedData0)
      val otherDf = df.withColumn("flattened", explode($"nested_list"))
      val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
      assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)
    }

  test("[Rewrite] Explode contains compatible field") {
    val df = getDataFrame(pathToNestedData0)
    val otherDf = df.withColumn("flattened", explode($"nested_list"))
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    assert(res.schema.filter(field => field.name.contains(Constants.COMPATIBLE_FIELD)).size == 2)
  }

  test("[Rewrite] Explode retains empty collection") {
    val df = getDataFrame(pathToNestedData00)
    val otherDf = df.withColumn("flattened", explode($"nested_list"))
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    assert(res.filter($"flat_key" === "6_flat_val_y").count() == 1)
  }

  test("[Rewrite] Explode retains element with null value") {
    val df = getDataFrame(pathToNestedData00)
    val otherDf = df.withColumn("flattened", explode($"nested_list"))
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    assert(res.filter($"flat_key" === "5_flat_val_y").count() == 1)
  }

}
