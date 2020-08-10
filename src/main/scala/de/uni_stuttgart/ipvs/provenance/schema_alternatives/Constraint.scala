package de.uni_stuttgart.ipvs.provenance.schema_alternatives

case class Constraint(constraintString: String, min: Int = 1, max: Int = -1) {

  val operatorId = getOperatorId(constraintString)
  val attributeValue = getAttributeValue(constraintString, operatorId)


  private def getPrefix(condition: String):String = {
    condition.substring(0, Math.min(8,condition.length))
  }

  private def getOperatorId(condition: String): Byte = {
    getPrefix(condition) match {
      case "contains" => 2
      case "ltltltlt" => 3
      case "gtgtgtgt" => 4
      case "nconncon" => 5
      case "lengthgt" => 6
      case "lengthlt" => 7
      case x => if (x.length() > 0) 1 else 0
    }
  }

  private def getAttributeValue(condition: String, operatorId: Byte): String = {
    if (operatorId < 2) condition else condition.substring(Math.min(8, condition.length-1))
  }

  def deepCopy(): Constraint = {
    Constraint(constraintString + "", min, max)
  }

}
