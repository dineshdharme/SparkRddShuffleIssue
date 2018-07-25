package spark.rddexample.dummyrdd.tree.child1.events


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import spark.rddexample.dummyrdd.tree.child1.Child1


object Function1 {
  def apply(child1: Child1): Unit = {

    val spark = SparkSession.builder.getOrCreate()


    val child2rdd_temp1 = child1.child2Element.rddval.map(row1 => Row(0,row1.get(0),row1.get(1), row1.get(9),0.0,0
    ))

    val child21rdd_temp1 = child1.child2Element.child21Element.rddval.map(row1 => Row(1,row1.get(0),row1.get(1), row1.get(9),1
    ))


    val child2Keyedrdd1 = child2rdd_temp1.keyBy(row => row(1))
    val child21Keyedrdd1 = child21rdd_temp1.keyBy(row => row(2))

    println("Printing child2Keyedrdd1 rows")
    //child2Keyedrdd1.take(2).foreach(println)

    println("Printing child21Keyedrdd1 rows")
    //child21Keyedrdd1.take(2).foreach(println)


    val temprdd  =child2Keyedrdd1.union(child21Keyedrdd1)

    val temprdd2  = temprdd.reduceByKey( (rowleft, rowright) => {

      if (rowleft.getAs[Int](0) ==  0 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright

        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3),
          row1.getAs[Double](4) + row2.getAs[Double](3),
          row1.getAs[Int](5) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 0) {
        val row1 = rowright
        val row2 = rowleft
        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3),
          row1.getAs[Double](4) + row2.getAs[Double](3),
          row1.getAs[Int](5) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright
        Row(row1.get(0), row1.get(1), row1.get(2),
          row1.getAs[Double](3) + row2.getAs[Double](3),
          row1.getAs[Int](4) + row2.getAs[Int](4)
        )
      }
      else {
        rowleft
      }

    })

    // currently the results are in (key,row) tuple format. we just need the final row.
    val temprdd4 = temprdd2.map(row => row._2)


    val temprdd3 = temprdd4.map(row1 => Row(1, row1.get(1), row1.get(2), row1.getAs[Double](3) +  row1.getAs[Double](4)/row1.getAs[Int](5).toDouble, 1
    ) )





    println("Printing temprdd3 rows")
    //temprdd3.take(10).foreach(println)



    val child1rdd_temp1 = child1.rddval.map(row1 => Row(0,row1.get(0), row1.get(1), row1.get(2), row1.get(3), row1.get(4), row1.get(5), row1.get(6),
      row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13),0.0,0
    ))


    val childprocessed1rdd_temp1 = temprdd3


    val child1Keyedrdd1 = child1rdd_temp1.keyBy(row => row(1))
    val childprocessed1Keyedrdd2 = childprocessed1rdd_temp1.keyBy(row => row(2))

    println("Printing child1Keyedrdd1 rows")
    //child1Keyedrdd1.take(2).foreach(println)

    println("Printing childprocessed1Keyedrdd2 rows")
    //childprocessed1Keyedrdd2.take(2).foreach(println)


    val temprdd6  =childprocessed1Keyedrdd2.union(child1Keyedrdd1)

    val temprdd7  = temprdd6.reduceByKey( (rowleft, rowright) => {

      if (rowleft.getAs[Int](0) ==  0 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright

        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3), row1.get(4), row1.get(5), row1.get(6),
          row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13), row1.get(14),
          row1.getAs[Double](15) + row2.getAs[Double](3),
          row1.getAs[Int](16) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 0) {
        val row1 = rowright
        val row2 = rowleft
        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3), row1.get(4), row1.get(5), row1.get(6),
          row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13), row1.get(14),
          row1.getAs[Double](15) + row2.getAs[Double](3),
          row1.getAs[Int](16) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright
        Row(row1.get(0), row1.get(1), row1.get(2),
          row1.getAs[Double](3) + row2.getAs[Double](3),
          row1.getAs[Int](4) + row2.getAs[Int](4)
        )
      }
      else {
        rowleft
      }

    })

    // currently the results are in (key,row) tuple format. we just need the final row.
    val temprdd8 = temprdd7.map(row => row._2)


    val temprdd9 = temprdd8.map(row1 => Row( row1.get(1), row1.get(2), row1.get(3), row1.get(4), row1.getAs[Double](5) + row1.getAs[Double](15)/row1.getAs[Int](16).toDouble ,
       row1.get(6), row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13), row1.get(14)
    ) )



    println("Printing temprdd9 rows")
    //temprdd9.take(10).foreach(println)






    val child2rdd_temp2 = child1.child2Element.rddval.map(row1 => Row(0, row1.get(0),row1.get(1), row1.get(9),0.0,0
    ))

    val child22rdd_temp1 = child1.child2Element.child22Element.rddval.map(row1 => Row(1 , row1.get(0),row1.get(1), row1.get(9),1
    ))


    val child2Keyedrdd3 = child2rdd_temp2.keyBy(row => row(1))
    val child22Keyedrdd1 = child22rdd_temp1.keyBy(row => row(2))

    println("Printing child2Keyedrdd3 rows")
    //child2Keyedrdd3.take(2).foreach(println)

    println("Printing child22Keyedrdd1 rows")
    //child22Keyedrdd1.take(2).foreach(println)


    val temprdd11  =child2Keyedrdd3.union(child22Keyedrdd1)

    val temprdd12  = temprdd11.reduceByKey( (rowleft, rowright) => {

      if (rowleft.getAs[Int](0) ==  0 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright

        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3),
          row1.getAs[Double](4) + row2.getAs[Double](3),
          row1.getAs[Int](5) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 0) {
        val row1 = rowright
        val row2 = rowleft
        Row(row1.get(0), row1.get(1), row1.get(2),row1.get(3),
          row1.getAs[Double](4) + row2.getAs[Double](3),
          row1.getAs[Int](5) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright
        Row(row1.get(0), row1.get(1), row1.get(2),
          row1.getAs[Double](3) + row2.getAs[Double](3),
          row1.getAs[Int](4) + row2.getAs[Int](4)
        )
      }
      else {
        rowleft
      }

    })

    // currently the results are in (key,row) tuple format. we just need the final row.
    val temprdd14 = temprdd12.map(row => row._2)


    val temprdd13 = temprdd14.map(row1 => Row(1, row1.get(1), row1.get(2), row1.getAs[Double](3) + row1.getAs[Double](4)/row1.getAs[Int](5).toDouble, 1
    ) )





    println("Printing temprdd13 rows")
    //temprdd13.take(10).foreach(println)



    val child1rdd_temp2 = child1.rddval.map(row1 => Row(0, row1.get(0), row1.get(1), row1.get(2), row1.get(3), row1.get(4), row1.get(5), row1.get(6),
      row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13),0.0,0
    ))


    val child2processedrdd_temp2 = temprdd13


    val child1Keyedrdd2 = child1rdd_temp2.keyBy(row => row(1))
    val child2processedKeyedrdd2 = child2processedrdd_temp2.keyBy(row => row(2))

    println("Printing child1Keyedrdd2 rows")
    //child1Keyedrdd2.take(2).foreach(println)

    println("Printing child2processedKeyedrdd2 rows")
    //child2processedKeyedrdd2.take(2).foreach(println)


    val temprdd16  =child2processedKeyedrdd2.union(child1Keyedrdd2)

    val temprdd17  = temprdd16.reduceByKey( (rowleft, rowright) => {

      if (rowleft.getAs[Int](0) ==  0 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright

        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3), row1.get(4), row1.get(5), row1.get(6),
          row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13), row1.get(14),
          row1.getAs[Double](15) + row2.getAs[Double](3),
          row1.getAs[Int](16) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 0) {
        val row1 = rowright
        val row2 = rowleft
        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3), row1.get(4), row1.get(5), row1.get(6),
          row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13),row1.get(14),
          row1.getAs[Double](15) + row2.getAs[Double](3),
          row1.getAs[Int](16) + row2.getAs[Int](4)
        )
      }
      else if(rowleft.getAs[Int](0) ==  1 && rowright.getAs[Int](0) == 1) {
        val row1 = rowleft
        val row2 = rowright
        Row(row1.get(0), row1.get(1), row1.get(2),
          row1.getAs[Double](3) + row2.getAs[Double](3),
          row1.getAs[Int](4) + row2.getAs[Int](4)
        )
      }
      else {
        rowleft
      }

    })

    // currently the results are in (key,row) tuple format. we just need the final row.
    val temprdd18 = temprdd17.map(row => row._2)


    val temprdd19 = temprdd18.map(row1 => Row( row1.get(1), row1.get(2), row1.get(3),
      row1.get(4),  row1.getAs[Double](5) + row1.getAs[Double](15)/row1.getAs[Int](16).toDouble, row1.get(6),
        row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13), row1.get(14)
    ) )



    println("Printing Temprdd2 rows")
    //temprdd19.take(10).foreach(println)




    val firstrdd1 = temprdd9.keyBy(row => row(0))
    val secondrdd1 = temprdd19.keyBy(row => row(0))

    println("Printing firstrdd1 rows")
    //firstrdd1.take(2).foreach(println)

    println("Printing secondrdd1 rows")
    //secondrdd1.take(2).foreach(println)


    val temprdd20  =firstrdd1.union(secondrdd1)

    val temprdd22  = temprdd20.reduceByKey( (rowleft, rowright) => {

        val row1 = rowleft
        val row2 = rowright

        Row(row1.get(0), row1.get(1), row1.get(2), row1.get(3),
          (row1.getAs[Double](4) + row2.getAs[Double](4)), row1.get(5), row1.get(6),
          row1.get(7),row1.get(8),row1.get(9),row1.get(10),row1.get(11),row1.get(12),row1.get(13)
        )


    })

    // currently the results are in (key,row) tuple format. we just need the final row.
    val temprdd23 = temprdd22.map(row => row._2)



    child1.rddval = temprdd23


    child1.parent.printRDDrows("child1.Function1")


  }
}
