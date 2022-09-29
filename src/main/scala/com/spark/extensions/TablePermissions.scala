package com.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.util.{Failure, Success, Try}

case class TablePermissions(spark: SparkSession) extends Rule[LogicalPlan] {

  def invokeMethodUsingReflection(o: Any, name: String): Any = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {

    case c: Command => c match {
      case c: CreateDataSourceTableAsSelectCommand =>
        c.copy(query = injectCustomUnaryNode(c.query))
      case cmd => cmd
    }
    case other => injectCustomUnaryNode(other)
  }

  private def getTablesFromPlan(plan: LogicalPlan): Map[LogicalPlan, CatalogTable] = {
    plan.collectLeaves().map {
      case h if h.nodeName == "HiveTableRelation" =>
        h -> invokeMethodUsingReflection(h, "tableMeta").asInstanceOf[CatalogTable]
      case m if m.nodeName == "MetastoreRelation" =>
        m -> invokeMethodUsingReflection(m, "catalogTable").asInstanceOf[CatalogTable]
      case l: LogicalRelation if l.catalogTable.isDefined =>
        l -> l.catalogTable.get
      case _ => null
    }.filter(_ != null).toMap
  }

  private def isPlanSafe(plan: LogicalPlan): Boolean = {
    val markNum = plan.collect { case _: customUnaryNode => true }.size
    markNum >= getTablesFromPlan(plan).size
  }


  private def injectCustomUnaryNode(plan: LogicalPlan): LogicalPlan = plan match {
    case rf: customUnaryNode => rf
    case plan if isPlanSafe(plan) => plan
    case other => customUnaryNode(other)
  }
}
