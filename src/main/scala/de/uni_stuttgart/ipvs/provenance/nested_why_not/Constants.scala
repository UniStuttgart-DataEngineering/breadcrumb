package de.uni_stuttgart.ipvs.provenance.nested_why_not

case object Constants {

  protected[provenance] val PROVENANCE_ID_STRUCT = "__PROVENANCE"
  protected[provenance] val VALID_FIELD = "__VALID"
  protected[provenance] val COMPATIBLE_FIELD = "__COMPATIBLE"
  protected[provenance] val SURVIVED_FIELD = "__SURVIVED"
  protected[provenance] val PROVENANCE_COLLECTION = "__PROVENANCE_COLLECTION"
  protected[provenance] val PROVENANCE_TUPLE = "__PROVENANCE_TUPLE"
  protected[provenance] val UDF_NAME = "__UDF"
  protected[provenance] val PROVENANCE_ID = "__ID"
  protected[provenance] val MAX_COL_FLATTEN = "__FLATTEN_MAX"

  protected def getUDFName(purpose: String) = {
    f"${UDF_NAME}_${purpose}"
  }

  protected[provenance] def getDataFetcherUDFName() = {
    getUDFName("data_fetcher")
  }

  protected[provenance] def getFlattenMaxColName(oid: Int) = {
    f"${MAX_COL_FLATTEN}_MAX_${oid}"
  }

  protected[provenance] def getFlattenArrayColName(oid: Int) = {
    f"${MAX_COL_FLATTEN}_ARRAY_${oid}"
  }

  protected[provenance] def getFlattenIdxColName(oid: Int) = {
    f"${MAX_COL_FLATTEN}_IDX_${oid}"
  }

  protected[provenance] def getFlattenUDFName() = {
    getUDFName("flatten_index")
  }

  private def getFieldName(fieldName: String, oid: Int): String = {
    f"${fieldName}_${oid}%04d"
  }

  private def getFieldName(fieldName: String, oid: Int, alternativeIdx: Int): String = {
    f"${getFieldName(fieldName, oid)}_${getAlternativeIdxString(alternativeIdx)}"
  }

  def getAlternativeIdxString(alternativeIdx: Int): String = {
    f"${alternativeIdx}%04d"
  }

  protected[provenance] def getSurvivorFieldName(oid: Int): String = {
    getFieldName(SURVIVED_FIELD, oid)
  }

  protected[provenance] def getSurvivorFieldName(oid: Int, alternativeIdx: Int): String = {
    getFieldName(SURVIVED_FIELD, oid, alternativeIdx)
  }

  protected[provenance] def getCompatibleFieldName(oid: Int): String = {
    getFieldName(COMPATIBLE_FIELD, oid)
  }

  protected[provenance] def getCompatibleFieldName(oid: Int, alternativeIdx: Int): String = {
    getFieldName(COMPATIBLE_FIELD, oid, alternativeIdx)
  }

  protected[provenance] def getProvenanceCollectionFieldName(oid: Int): String = {
    getFieldName(PROVENANCE_COLLECTION, oid)
  }

  protected[provenance] def getProvenanceTupleFieldName(oid: Int): String = {
    getFieldName(PROVENANCE_TUPLE, oid)
  }

  protected[provenance] def getValidFieldName(alternativeIdx: Int): String = {
    getFieldName(VALID_FIELD,0, alternativeIdx)
  }

  protected[provenance] def isNestedProvenanceCollection(name: String): Boolean = {
    name.contains(PROVENANCE_COLLECTION)
  }

  protected[provenance] def isSurvivedField(name: String): Boolean = {
    name.contains(SURVIVED_FIELD)
  }

  protected[provenance] def isIDField(name: String): Boolean = {
    name.contains(PROVENANCE_ID)
  }

  protected[provenance] def getAlternativeFieldName(name: String, oid: Int, alternativeIdx: Int): String = {
    getFieldName(name, oid, alternativeIdx)
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
