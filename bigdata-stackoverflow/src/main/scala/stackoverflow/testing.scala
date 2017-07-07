package stackoverflow

object testing {
  def main(args: Array[String]) = {

    //    var a = List((1, 2), (2, 3), (1, 3), (1, 1), (3, 5), (2, 1))
    //    var b = List(1, 3, 4)
    //    println(b.reduceLeft(_ + _))
    //    println(a)
    //
    //    println(a.groupBy(_._1))
    //    println(a.groupBy(_._1).mapValues(x => x.map(y => y._2)))
    //    println(a.groupBy(_._1).mapValues(x => x.map(y => y._2).reduce(_ + _)))
    //    println(a.groupBy(_._1).mapValues(x => x.map(y => y._2).reduce(_ + _)).toSeq)
    //
    //    var m1 = Map(1 -> 2, 3 -> 4)
    //    var m2 = Map(1 -> 3, 2 -> 3)
    //    println(m2 ++ m1)
    //
    //    var arr1 = Array((1, 1), (1, 2), (1, 2))
    //    arr1.foreach(println)
    //    arr1.distinct.foreach(println)
    val a = Array[(Int, Int)]((0, 100), (1, 11), (0, 13), (0, 14));
    val b = a.reduce((x, y) => (x._1, Math.max(x._2, y._2)))
    println(b)
    mergeArrays(Array[Int](1, 2, 3, 4, 5), a)

  }

  def mergeArrays(arr: Array[Int], newarr: Array[(Int, Int)]) = {
    val indexedCurrentCenters = arr.indices.map(i => (i, arr(i))).toMap
    val joinedcurrentCenterIndexToNewCenterMapping = (indexedCurrentCenters ++ newarr.toMap).toArray.sortBy(_._1)
    //    joinedcurrentCenterIndexToNewCenterMapping.foreach(x => println("joinedcurrentCenterIndexToNewCenterMapping = " + x))

    val newMeans = joinedcurrentCenterIndexToNewCenterMapping.map(x => x._2)
    newMeans.foreach(println)

  }
}