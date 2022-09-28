package com.databricks.extension

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

case class customUnaryNode(child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}