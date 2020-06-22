package de.uni_stuttgart.ipvs.provenance.nested_why_not

case object Constants {

  protected[provenance] val PROVENANCE_ID_STRUCT = "__PROVENANCE"
  protected[provenance] val VALID_FIELD = "__VALID"
  protected[provenance] val COMPATIBLE_FIELD = "__COMPATIBLE"
  protected[provenance] val SURVIVED_FIELD = "__SURVIVED"
  protected[provenance] val PROVENANCE_COLLECTION = "__PROVENANCE_COLLECTION"
  protected[provenance] val PROVENANCE_TUPLE = "__PROVENANCE_TUPLE"
  protected[provenance] val UDF_NAME = "__UDF"

  protected[provenance] def getUDFName = {
    UDF_NAME
  }

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

  protected[provenance] def getProvenanceTupleFieldName(oid: Int): String = {
    getFieldName(PROVENANCE_TUPLE, oid)
  }

  def columnNameContainsProvenanceConstant(name: String): Boolean = {
    var contains = false

    contains |= name.contains(PROVENANCE_ID_STRUCT)
    contains |= name.contains(VALID_FIELD)
    contains |= name.contains(COMPATIBLE_FIELD)
    contains |= name.contains(SURVIVED_FIELD)
    contains |= name.contains(PROVENANCE_COLLECTION)
    contains |= name.contains(PROVENANCE_TUPLE)

    contains
  }

}
