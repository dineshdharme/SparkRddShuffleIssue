package spark.rddexample.dummyrdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.io._


import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import spark.rddexample.dummyrdd.tree.parent.Parent



object FunctionExecutor {
  def main(args: Array[String]): Unit = {

    val veryStart = System.currentTimeMillis()

    val sparkConfig = new SparkConf()


    val spark = SparkSession
      .builder()
      .appName("FunctionExecutor")
      .config(sparkConfig)
      .getOrCreate()



    spark.sparkContext.setLogLevel("WARN")

    println("timesteps  - args(0)     = " + args(0))
    println("datapath   - args(1)     = " + args(1))
    println("report path - args(2)    = " + args(2))


    val hdfsCheckDir = "hdfs:///tree/dummy/data/checkpoint"
    spark.sparkContext.setCheckpointDir(hdfsCheckDir)


    //V.IMP This following setting could really affect the disk size required.
    // Toggle this setting in the final build.
    // TO control the disk space used by the parquet file blocks.
    val blockSize = 1024 * 1024 * 1

    val timesteps = args(0).toInt

    println("timesteps - Integer Value = " + timesteps)

    val reportPath = args(2) + "example_report.csv"

    val parent = new Parent(args(1))


    // Run event sequence over 60 time steps
    for (t <- 0 until timesteps) {


      println(">>>>>>>>>>>Looping through timestep = " + t + "  <<<<<<<<<<<<")

      val startMillis = System.currentTimeMillis()

      parent.child1Element.functions.applyFunction1()
      parent.child1Element.functions.applyFunction1()
      parent.child1Element.functions.applyFunction1()
      parent.child1Element.functions.applyFunction1()


      val endMillis = System.currentTimeMillis()

      val diff = (endMillis - startMillis) / 1000.0

      println("LoopNo : " + t + " TimeTaken (seconds): " + diff.toString)



    }


    val child1rdd  = parent.child1Element.rddval.keyBy(row => row(0))

    val child2rdd = parent.child1Element.child2Element.rddval.keyBy(row => row(1))


    val intermediate = child1rdd.join(child2rdd)


    val intermediate2 = intermediate.map(value => value._2)

    println("Printing intermediate2 rows")
    intermediate2.take(10).foreach(println)

    val intermediate3 = intermediate2.keyBy(value => value._1(1))

    println("Printing intermediate3 rows")
    intermediate3.take(10).foreach(println)


    val parentKeyedrdd = parent.rddval.keyBy(row => row(0))

    val intermediate4 = parentKeyedrdd.join(intermediate3)

    val intermediate5 = intermediate4.map(value => (value._2._1,value._2._2._1,value._2._2._2))

    println("Printing intermediate5 rows")
    intermediate5.take(10).foreach(println)


    val intermediate6 = intermediate5.map( value => Row(value._1(0),value._2(0),value._3(0),
      value._1(4),value._1(5),value._1(6),value._1(2),value._1(3),
      value._2(6),
      value._3(2),value._3(6)))


    val resultSchema = StructType(
      StructField("element_id", StringType, nullable = false)::
        StructField("child1_element_id", StringType, nullable = false)::
        StructField("child2_element_id", StringType, nullable = false)::
        StructField("num1", DoubleType, nullable = false)::
        StructField("num2", DoubleType, nullable = false)::
        StructField("num3", DoubleType, nullable = false)::
        StructField("cat1", StringType, nullable = false)::
        StructField("cat2", StringType, nullable = false)::
        StructField("child1_num1", DoubleType, nullable = false)::
        StructField("child2_cat1", StringType, nullable = false)::
        StructField("child2_num3", DoubleType, nullable = false)::
        Nil
    )
    val finalResult = spark.sqlContext.createDataFrame(intermediate6, resultSchema)

    println("Writing data to csv file")
    finalResult.coalesce(1).write.option("header", "true").mode("overwrite").csv(reportPath)



    val veryEnd = System.currentTimeMillis()

    val diffTotal = (veryEnd - veryStart) / 1000.0

    println("Total Time taken : " +  diffTotal.toString)

    //Stop the sparkcontext
    spark.sparkContext.stop()

  }
}
