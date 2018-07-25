package spark.rddexample.dummyrdd.tree.child22


import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import spark.rddexample.TreeElement
import spark.rddexample.dummyrdd.tree.child2.Child2


class Child22(val child2: Child2) extends TreeElement(

  dataSourceName = child2.child1.parent.path +  "Child22.csv",
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



}
