package spark.rddexample.dummyrdd.tree.child2


import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import spark.rddexample.TreeElement
import spark.rddexample.dummyrdd.tree.child1.Child1
import spark.rddexample.dummyrdd.tree.child21.Child21
import spark.rddexample.dummyrdd.tree.child22.Child22


class Child2(val child1: Child1) extends TreeElement(
  dataSourceName = child1.parent.path +  "Child2.csv",
  dataSourceSchema = StructType(
    StructField("element_id", StringType, nullable = false)::
      StructField("parentelement_id", StringType, nullable = false)::
      StructField("cat1", StringType, nullable = false)::
      StructField("cat2", StringType, nullable = false)::
      StructField("num1", DoubleType, nullable = false)::
      StructField("num2", DoubleType, nullable = false)::
      StructField("num3", DoubleType, nullable = false)::
      StructField("num4", DoubleType, nullable = false)::
      StructField("num5", DoubleType, nullable = false)::
      StructField("num6", DoubleType, nullable = false)::
      StructField("num7", DoubleType, nullable = false)::
      StructField("num8", DoubleType, nullable = false)::
      StructField("num9", DoubleType, nullable = false)::
      StructField("num10", DoubleType, nullable = false)::
      Nil
  )
) {


  // Children DataBoxes
  val child21Element = new Child21(this)
  val child22Element = new Child22(this)
}
