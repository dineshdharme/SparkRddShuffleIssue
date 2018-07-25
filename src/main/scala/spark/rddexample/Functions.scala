package spark.rddexample

import org.apache.spark.sql.functions._

trait Functions {

  def runFunction[A <: TreeElement](element: A, fn: Function[A, Unit]): A = {

    val startMillis = System.currentTimeMillis()

    fn(element)

    val endMillis = System.currentTimeMillis()

    val diff = (endMillis - startMillis) / 1000.0

    println("TimeTaken (seconds): " + diff.toString)


    element
  }
}
