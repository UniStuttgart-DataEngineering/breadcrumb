package de.uni_stuttgart.ipvs.provenance.nested_why_not

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

case class Rewrite(
                    plan: LogicalPlan,
                    provenanceExtension: ProvenanceContext) {}
