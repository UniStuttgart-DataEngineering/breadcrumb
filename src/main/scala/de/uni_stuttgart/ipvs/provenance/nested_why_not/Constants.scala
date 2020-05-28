package de.uni_stuttgart.ipvs.provenance.nested_why_not

case object Constants {

  val PROVENANCE_ID_STRUCT = "__PROVENANCE"
  val PROVENANCE_ID_FIELD = "__PID"
  val FILTER_OP = "__Fi"
  val VALID_FIELD = "__VALID"
  val COMPATIBLE_FIELD = "__COMPATIBLE"
  val SURVIVED_FIELD = "__SURVIVED"
  val PROVENANCE_COLLECTION = "__PROVENANCE_COLLECTION"

  private def getFieldName(fieldName: String, oid: Int): String = {
    f"${fieldName}_${oid}%04d"
  }

  protected[provenance] def getSurvivorFieldName(oid: Int): String = {
    getFieldName(SURVIVED_FIELD, oid)
  }

  protected[provenance] def getCompatibleFieldName(oid: Int): String = {
    getFieldName(COMPATIBLE_FIELD, oid)
  }

  protected[provenance] def getProvenanceCollectionFieldName(oid: Int): String = {
    getFieldName(PROVENANCE_COLLECTION, oid)
  }



}
