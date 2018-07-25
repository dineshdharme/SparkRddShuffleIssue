package spark.rddexample.dummyrdd.tree.parent


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, first, sum}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import java.util.UUID.randomUUID

import org.apache.spark.util.SizeEstimator
import org.apache.spark.storage.StorageLevel
import spark.rddexample.TreeElement
import spark.rddexample.dummyrdd.tree.child1.Child1

class Parent(val path : String) extends TreeElement(
  dataSourceName = path +  "Parent.csv",
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
  val child1Element = new Child1(this)






  def printRDDrows(eventName : String): Unit = {

    println("In Function : " + eventName)


    println("Caching  all Rdds")

    rddval.cache()
    child1Element.rddval.cache()
    child1Element.child2Element.rddval.cache()
    child1Element.child2Element.child21Element.rddval.cache()
    child1Element.child2Element.child22Element.rddval.cache()



    println("Materializing all RDDs")
    println(" Parent Rows  :"  + rddval.count() + "      columns : " + rddval.first().length )
    println(" Child1 Rows  :"  + child1Element.rddval.count() + "     columns : " + child1Element.rddval.first().length)
    println(" Child2 Rows  :"  + child1Element.child2Element.rddval.count() + "     columns : " + child1Element.child2Element.rddval.first().length)
    println(" Child21 Rows :"  + child1Element.child2Element.child21Element.rddval.count() + "   columns : " + child1Element.child2Element.child21Element.rddval.first().length)
    println(" Child22 Rows :"  + child1Element.child2Element.child22Element.rddval.count() + "   columns : " + child1Element.child2Element.child22Element.rddval.first().length)



  }


}
