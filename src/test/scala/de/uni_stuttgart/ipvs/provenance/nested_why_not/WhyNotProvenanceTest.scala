package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.{SharedSparkTestDataFrames}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Literal}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.Assertions._

class WhyNotProvenanceTest extends FunSuite with SharedSparkTestDataFrames{

  import spark.implicits._

  test("The first test, fail fast, fail often") {
    var df = singleInputColumnDataFrame()
    df = df.filter($"MyIntCol" > 2)
    //df.explain(true)
    // TODO: provenance question
    val pq = $"MyIntCol".equalTo(Literal("1"))
    df = WhyNotProvenance.annotate(df, pq.expr)
    //df.explain(true)
    df.show
    val res = df.collect()
//    assert(res.length == 4)
  }

}
