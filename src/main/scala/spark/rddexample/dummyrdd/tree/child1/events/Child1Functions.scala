package spark.rddexample.dummyrdd.tree.child1.events


import spark.rddexample.Functions
import spark.rddexample.dummyrdd.tree.child1.Child1

class Child1Functions(business: Child1) extends Functions {
  def applyFunction1(): Child1 = runFunction(business, Function1.apply)

}
