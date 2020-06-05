package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.scalatest.FunSuite

class JoinTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer {

  import spark.implicits._

  def basicWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  test("[Rewrite] Join adds survived column") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0)
    val df = dfLeft.join(dfRight, Seq("key"))
    val res = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)

  }

  test("[Rewrite] Rewritten join marks all items that do not survive the original join with false") {

  }

  test("[Rewrite] Rewritten join retains previous ") {

  }



}
