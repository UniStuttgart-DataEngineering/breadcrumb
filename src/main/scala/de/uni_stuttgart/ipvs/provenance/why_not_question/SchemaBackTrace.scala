package de.uni_stuttgart.ipvs.provenance.why_not_question

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaNode, SchemaSubsetTree}
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CreateNamedStruct, Explode, Expression, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Generate, Join, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable


object SchemaBackTrace {
  def apply(plan: LogicalPlan, whyNotQuestion:SchemaSubsetTree)  = new SchemaBackTrace(plan, whyNotQuestion)
}


class SchemaBackTrace(plan: LogicalPlan, whyNotQuestion: SchemaSubsetTree) {
  var unrestructuredWhyNotQuestionInput: SchemaSubsetTree = whyNotQuestion.deepCopy()
  var unrestructuredWhyNotQuestionInputRight: SchemaSubsetTree = whyNotQuestion.deepCopy()
  var unrestructuredWhyNotQuestionOutput: SchemaSubsetTree = whyNotQuestion.deepCopy()
  var listOfWhyNotQuestionInput = Seq[SchemaSubsetTree]()

  def unrestructure(): Seq[SchemaSubsetTree] = {
    // Create a new tree for unrestructuring the question
    val newRoot = whyNotQuestion.rootNode.deepCopy(null)
    val newRootR = whyNotQuestion.rootNode.deepCopy(null)

    newRoot.children.clear()
    newRootR.children.clear()

    unrestructuredWhyNotQuestionInput.rootNode = newRoot
    unrestructuredWhyNotQuestionInputRight.rootNode = newRootR

    // Map an expression to its name
    val exprToName = scala.collection.mutable.Map[Expression,String]()

    // Analyze the plan to unrestructure the question
    analyzePlan(plan, newRoot, exprToName)

    // Return the unrestructured question
    listOfWhyNotQuestionInput = listOfWhyNotQuestionInput :+ unrestructuredWhyNotQuestionInput

    if(!unrestructuredWhyNotQuestionInputRight.rootNode.children.isEmpty)
      listOfWhyNotQuestionInput = listOfWhyNotQuestionInput :+ unrestructuredWhyNotQuestionInputRight

    listOfWhyNotQuestionInput
  }

  def analyzePlan(plan: LogicalPlan, newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]): SchemaNode = {
    plan match {
      case p: Project => analyzeProject(p, newRoot, exprToName)
      case f: Filter => analyzeFilter(f, newRoot, exprToName)
      case j: Join => analyzeJoin(j, newRoot, exprToName)
      case _ => newRoot.copyNode(unrestructuredWhyNotQuestionOutput.rootNode)
    }

    newRoot
  }

  def analyzeProject(p: Project, newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]) = {
    val pChild = p.child
    pChild match {
      // TODO: Flatten comes as Project (better way to analyze)
      case g: Generate => unrestructureGenerate(g, newRoot)
      /*
        TODO: Join may come as Project (better way to analyze)
              For now, we define the plan as a Join if the plan is Project with no renaming and the child is Join
       */
      case j: Join => {
        var projList = List[String]()

        for(ar <- j.left.output)
          if (ar.isInstanceOf[AttributeReference] && !projList.contains(ar.name))
            projList = projList :+ ar.name

        for(ar <- j.right.output)
          if (ar.isInstanceOf[AttributeReference] && !projList.contains(ar.name))
            projList = projList :+ ar.name

        if(projList.size == p.projectList.size)
          analyzeJoin(j, newRoot, exprToName)
        else
          unrestructureProject(p, newRoot, exprToName)
      }
      case _ => unrestructureProject(p, newRoot, exprToName)
    }
  }

  def analyzeFilter(f:Filter, newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]) = {
    // Filter may occur over another operator or base relation
    val fChild = f.child
    fChild match {
      case l: LeafNode => unrestructureFilter(l, newRoot)
//      case j: Join => {
//        newRoot.copyNode(unrestructuredWhyNotQuestionOutput.rootNode)
//        analyzeJoin(j, newRoot, exprToName)
//      }
      case _ => newRoot.copyNode(unrestructuredWhyNotQuestionOutput.rootNode)
    }
  }

  def analyzeJoin(j: Join, newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]) = {
    checkConstraintConsistency(j)
    unrestructureJoin(j, newRoot, exprToName)
  }

  def unrestructureGenerate(generate: Generate, newRoot: SchemaNode): SchemaNode = {
    for (ex <- generate.generatorOutput) {
      ex match {
        case ar: AttributeReference => unrestructureAttributeRefForUnnesting(ar, newRoot, generate)
        case _ => // do nothing
      }
    }

    newRoot
  }

  def unrestructureProject(project: Project, newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]): SchemaNode = {
    for (ex <- project.projectList) {
      ex match {
        case ar: AttributeReference => unrestructureAttributeRef(ar, newRoot)
        case a: Alias => {
          // Access the node corresponding to the renamed attribute in the why-not question
          val node = whyNotQuestion.getNodeByName(a.name)
          /*
            Parameters:
              1) Sub-expressions in projection operator
              2) A tree that the root is the attribute corresponding to the renamed attribute
              3) A new tree for creating the unrestructured why-not question
              4) A map: key = an expression and value = the name for the expression
            How it works:
              1) Recursively access child nodes in the tree for the why-not question to find any renamed and restructured attribute
              2) Create a new tree for the why-not question based on the sub-expressions from the logical plan as well as why-not question schema
           */
          unrestructureSubExpressions(a.children, node, newRoot, exprToName)
        }
        case MultiAlias(child, names) =>
        case UnresolvedAlias(child, aliasFunc) => {
          //maybe we do not want to support this at the beginning?
        }
      }
    }

    newRoot
  }

  def unrestructureFilter(relation: LeafNode, newRoot: SchemaNode): SchemaNode = {
    for(ex <- relation.schema) {
      ex match {
        case st: StructField => unrestructureStructField(st, newRoot)
        case _ => // do nothing
      }
    }

    newRoot
  }

  def unrestructureJoin(j: Join, newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]): SchemaNode = {
    val lPlan = j.left
    val rPlan = j.right

    lPlan match {
      case lr: LogicalRelation => {
        // Generate a tree for left
        for(lAr <- lr.output)
          unrestructureAttributeRef(lAr, newRoot)
      }
      case p: Project => unrestructureProject(p, newRoot, exprToName)
      case _ =>
    }

    rPlan match {
      case lr: LogicalRelation => {
        // Generate a tree for right
        for(rAr <- lr.output)
          unrestructureAttributeRef(rAr, unrestructuredWhyNotQuestionInputRight.rootNode)
      }
      case p: Project => unrestructureProject(p, unrestructuredWhyNotQuestionInputRight.rootNode, exprToName)
      case _ =>
    }

    newRoot
  }

  def unrestructureAttributeRefForUnnesting(ar: AttributeReference, newRoot: SchemaNode, newGenerate: Generate): SchemaNode = {
    val node = whyNotQuestion.getNodeByName(ar.name)
    val parent = newGenerate.generator.asInstanceOf[Explode]

    // Recursively unrestructure if multiple collections are flattened
    if (newGenerate.child.isInstanceOf[Project]) {
      val projectOp = newGenerate.child.asInstanceOf[Project]
      val generateOp = projectOp.child.asInstanceOf[Generate]
      val innerAttrRef = generateOp.generatorOutput.head.asInstanceOf[AttributeReference]
      unrestructureAttributeRefForUnnesting(innerAttrRef, newRoot, generateOp)
    }

    val parentAsAttrRef = parent.child.asInstanceOf[AttributeReference]
    val newNode: SchemaNode = SchemaNode(parentAsAttrRef.name)

    if (node != null) {
      val newChildNode: SchemaNode = SchemaNode("")
      newChildNode.copyNode(node)
      newChildNode.name = "element"

      unrestructuredWhyNotQuestionOutput.rootNode.copyNode(newChildNode)
      newChildNode.children.clear()

      if (ar.dataType.typeName.equals("struct")) {
        val fields = ar.dataType.asInstanceOf[StructType]
        var newFields = Array[StructField]()

        for (f <- fields) {
          val childName = node.children.find(node => node.name == f.name).getOrElse("")
          if (!childName.equals("")) newFields = newFields :+ f

//          //TODO: matching to input schema
//          newFields = newFields :+ f
        }

        getAllStructFields(StructType(newFields), newChildNode)
      }

      // Set element node
      newNode.addChild(newChildNode)
      newChildNode.setParent(newNode)

      // Add to newRoot
      newRoot.addChild(newNode)
      newNode.setParent(newRoot)
    }

    newRoot
  }

  def unrestructureStructField(st: StructField, newRoot: SchemaNode): SchemaNode = {
    val node = whyNotQuestion.getNodeByName(st.name)
    val newNode: SchemaNode = SchemaNode(st.name)

    if (node != null) {
      if (st.dataType.typeName.equals("struct")) {
        newNode.deepCopy(node)

        if (node.children.isEmpty) {
          getAllStructFields(st.dataType.asInstanceOf[StructType], newNode)
        } else {
          val fields = st.dataType.asInstanceOf[StructType]
          var newFields = Array[StructField]()

          for (f <- fields) {
            val childName = node.children.find(node => node.name == f.name).getOrElse("")
            if (!childName.equals("")) newFields = newFields :+ f
          }

          getAllStructFields(StructType(newFields), newNode)
        }
      } else {
        newNode.copyNode(node)
      }

      // Add to newRoot
      newRoot.addChild(newNode)
      newNode.setParent(newRoot)
    }

    newRoot
  }

  def unrestructureAttributeRef(ar: AttributeReference, newRoot: SchemaNode): SchemaNode = {
    val node = whyNotQuestion.getNodeByName(ar.name)
    val newNode: SchemaNode = SchemaNode(ar.name)

    if (node != null) {
      if (ar.dataType.typeName.equals("struct")) {
        newNode.deepCopy(node)

        if (node.children.isEmpty) {
          getAllStructFields(ar.dataType.asInstanceOf[StructType], newNode)
        } else {
          val fields = ar.dataType.asInstanceOf[StructType]
          var newFields = Array[StructField]()

          for (f <- fields) {
            val childName = node.children.find(node => node.name == f.name).getOrElse("")
            if (!childName.equals("")) newFields = newFields :+ f
          }

          getAllStructFields(StructType(newFields), newNode)
        }
      } else {
        newNode.copyNode(node)
      }

      // Add to newRoot
      newRoot.addChild(newNode)
      newNode.setParent(newRoot)
    }

    newRoot
  }

  def unrestructureSubExpressions(expressions: Seq[Expression], renamedNode: SchemaNode,
                                  newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]): SchemaNode = {
    for (eachExp <- expressions) {
      eachExp match {
        case lt: Literal => // do nothing
        case gsf: GetStructField => unrestructureSubExpressions(gsf.children, renamedNode, newRoot, exprToName)
        case ar: AttributeReference => unrestructureAttributeRefFromSubExpressions(ar, renamedNode, newRoot, exprToName)
        case cns: CreateNamedStruct => {
          var pos = 0

          // Map expression to its name (inner Alias is ignored in the plan)
          for (each <- cns.valExprs) {
            val name = cns.names(pos).toString
            exprToName.put(each,name)

            // Further collect descendant if the attribute is nested
            each match {
              case gsf: GetStructField => exprToName.put(gsf.child, gsf.name.get)
              case _ => // do nothing
            }
            pos += 1
          }

          // Access to the inner renamed child node (attribute)
          if (pos > 0) {
            val name = exprToName.get(cns).getOrElse("")

            if (!name.equals("")) {
              val node = renamedNode.getChild(name).getOrElse(null)
              if (node != null) renamedNode.copyNode(node)
            }
          }

          unrestructureSubExpressions(cns.valExprs, renamedNode, newRoot, exprToName)
        }
      }
    }

    newRoot
  }

  def unrestructureAttributeRefFromSubExpressions(ar: AttributeReference, renamedNode: SchemaNode,
                                  newRoot: SchemaNode, exprToName: mutable.Map[Expression,String]): SchemaNode = {
    // Create a new node
    val newNode: SchemaNode = SchemaNode(ar.name)

    /*
      1) Create a node "A" corresponding to the original attribute from the input
      2) Correctly create or copy all the descendants of "A"
      3) Add the tree rooted "A" to the new tree for the why-not question
     */
    if(renamedNode != null) {
      if (renamedNode.children.isEmpty) {
        val leafNode = newRoot.getLeafNode()

        if (renamedNode.constraint.attributeValue != "")
          newNode.constraint = renamedNode.constraint.deepCopy()

        if (!leafNode.name.equals(newRoot.name)) {
          if(ar.dataType.typeName.equals("struct")) {
            leafNode.addChild(newNode)
            newNode.setParent(leafNode)
            newNode.copyNode(leafNode)
          }
        }
      } else {
        val node = renamedNode.getChild(ar.name).getOrElse(null)

        if (node != null) {
          newNode.copyNode(node)
        } else {
          newNode.name = ""

          val doubleCheck = exprToName(ar)
          if (doubleCheck != "" && !doubleCheck.equals(ar.name)) {
            val doubleCheckNode = renamedNode.getChild(doubleCheck).getOrElse(null)
            renamedNode.copyNode(doubleCheckNode)
            val node = renamedNode.getChild(ar.name).getOrElse(null)

            if (node != null) {
              newNode.copyNode(doubleCheckNode)
            } else {
              newNode.name = ar.name
            }
          }
        }
      }
    } else {
      newNode.name = ""
    }

    if (newNode.name != "") {
      if (ar.dataType.typeName.equals("struct")) {
        getAllStructFields(ar.dataType.asInstanceOf[StructType], newNode)
      }

      newRoot.addChild(newNode)
      newNode.setParent(newRoot)
    }

    newRoot
  }

  def getAllStructFields(st: StructType, newNode: SchemaNode): SchemaNode = {
    for (eachStruct <- st) {
      val structNode = SchemaNode("")

      if (eachStruct.dataType.typeName.equals("struct")) {
        getAllStructFields(eachStruct.dataType.asInstanceOf[StructType], structNode)
      }

      structNode.name = eachStruct.name
      newNode.addChild(structNode)
      structNode.setParent(newNode)
    }

    newNode
  }

  def checkConstraintConsistency(j: Join) ={
    for (c <- j.condition) {
      val leftNode = whyNotQuestion.getNodeByName(c.children.head.asInstanceOf[AttributeReference].name)
      val rightNode = whyNotQuestion.getNodeByName(c.children.last.asInstanceOf[AttributeReference].name)

      if (leftNode != null && rightNode != null) {
        if (leftNode.constraint.constraintString == "")
          leftNode.constraint = rightNode.constraint.deepCopy()

        if (rightNode.constraint.constraintString == "")
          rightNode.constraint = leftNode.constraint.deepCopy()

        if (leftNode.constraint != "" && rightNode.constraint != "")
          assert(leftNode.constraint == rightNode.constraint)
      }
    }
  }
}
