package spark.rddexample

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD


abstract class TreeElement(dataSourceName: String,
                           dataSourceSchema: StructType
                       ) {


  var rddval : RDD[Row] = createRDD()

  private def createRDD(): RDD[Row] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val rawDF = spark.read
      .schema(dataSourceSchema)
      .option("mode", "FAILFAST")
      .option("header", "true")
      .csv(dataSourceName).rdd



    rawDF
  }


}
