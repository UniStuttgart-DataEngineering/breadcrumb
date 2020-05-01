package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.Map

class Schema (dataFrame: DataFrame) {

  val labeles = parseSchemaOfDataFrame(dataFrame)
  val nameStreams = initializeNameStreams(labeles)

  def parseSchemaOfDataFrame(dataFrame: DataFrame): Map[String, String] = {
    var schemaElementsWithLabels = Map.empty[String, String]
    schemaElementsWithLabels.put("0", "root")


    val dfSchema = dataFrame.schema
    val fields = dfSchema.fields

    addLabels(fields, schemaElementsWithLabels, "0")
    schemaElementsWithLabels
  }

  // end-recursive, side effects on schemaElementsWithLabels
  def addLabels(schemaElements: Array[StructField], schemaElementsWithLabels: Map[String, String], parentLabel: String) : Unit = {
    var labelCounter = 0
    for ( field <- schemaElements){
      val currentLabel = parentLabel + "." + labelCounter.toString()
      schemaElementsWithLabels.put(currentLabel, field.name)
      field.dataType match {
        case nestedStructuredAttribute : StructType => {
          addLabels(nestedStructuredAttribute.fields, schemaElementsWithLabels, currentLabel)
        }
        case nestedArrayAttribute : ArrayType => {
          val helperSubField = StructField("element", nestedArrayAttribute.elementType, field.nullable)
          addLabels(Array(helperSubField), schemaElementsWithLabels, currentLabel)
        }
        case _ => {}
      }
      labelCounter += 1
    }
  }

  def initializeNameStreams(labeledSchema: Map[String, String]) : Map[String, Seq[String]] = {
    val nameStreams = Map.empty[String, Seq[String]]
    for ((label, name) <- labeledSchema) {
      nameStreams.get(name) match {
        case Some(hit : Seq[String]) => nameStreams.update(name, hit :+ label)
        case None => {val labels = Seq(label)
          nameStreams += (name -> labels)}
      }
    }
    nameStreams
  }

  def getNameStream(name: String) : Seq[String] = {
    nameStreams.getOrElse(name, Seq.empty[String])
  }

  def getAncestors(label: String) : Seq[String] = {
    var ancestors = Seq.empty[String]
    var parts = label.split('.')
    parts = parts.dropRight(1)
    while (parts.nonEmpty) {
      val ancestorLabel = parts.mkString(".")
      val ancestorName = labeles.get(ancestorLabel)
      if (ancestorName.isDefined) {
        ancestors = ancestors :+ ancestorName.get
      } else {
        throw new RuntimeException("Could not find node with the according label: " + label)
      }
      parts = parts.dropRight(1)
    }
    return ancestors
  }

  def getTrailingNodeIDs(ancestorLabel: String, descendantLabel: String): Seq[String] = {
    descendantLabel.stripPrefix(ancestorLabel).stripPrefix(".").split('.')

  }

  def getCommonLabelPrefix(labels : Seq[String]): String ={
    if (labels.nonEmpty){
      var prefix = labels(0)
      for (label <- labels){
        prefix = pref(prefix, label)
      }
      prefix.stripSuffix(".")
    } else {
      ""
    }
  }

  private def pref(s: String, t: String, out: String = ""): String = {
    if (s == "" || t == "" || s(0) != t(0)) out
    else pref(s.substring(1), t.substring(1), out + s(0))
  }

  def getName(label: String): Option[String] ={
    this.labeles.get(label)
  }

  @deprecated
  def getSparkAccessPath(label: String): Option[String] = {
    getName(label) match {
      case Some(_) => Some(dissolveSparkAccessPath(label).replaceAll("""\.element""", ""))
      case None => None
    }
  }

  def getSchemaPath(label: String): Option[Seq[String]] = {
    getName(label) match {
      case Some(_) => Some(dissolveSchemaPath(label))
      case None => None
    }
  }

  def isNested(label: String) : Boolean = {
    getName(label) match {
      case Some(_) => dissolveSparkAccessPath(label).contains("element")
      case None =>false
    }
  }

  private def dissolveSchemaPath(label: String) : Seq[String] = {
    val parts = label.split('.')
    var currentPath = mutable.Seq.empty[String]
    var currentParts = mutable.Seq.empty[String]
    for (part <- parts){
      currentParts :+= part
      val currentLabel = currentParts.mkString(".")
      val currentName = getName(currentLabel).get
      currentPath :+= currentName
    }
    currentPath.toList
  }


  private def dissolveSparkAccessPath(label: String) : String = {
    val parts = label.split('.')
    var currentLabel = parts(0) //should always be the root element, thus we do not need to resolve it for later spark usage
    var currentPath = ""
    for (part <- parts.drop(1)){
      currentLabel += "." + part
      val currentName = getName(currentLabel).get
      //if (currentName != "element") {//dedicated name for elements in nested collections
      currentPath += "." + getName(currentLabel).get
      //}
    }
    currentPath.drop(1)
  }
}
