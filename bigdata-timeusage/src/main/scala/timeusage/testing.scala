package timeusage

import java.text.DecimalFormat

object testing {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    import org.apache.spark._
    import org.apache.spark.sql.SQLContext

    val conf: SparkConf = new SparkConf().setAppName("Mylocalstest").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
 
    val input = sc.parallelize(Seq(
      ("a", 5, 7, 9, 12, 13),
      ("b", 6, 4, 3, 30, 17),
      ("c", 4, 9, 5, 6, 9),
      ("d", 4, 2, 6, 8, 1))).toDF("ID", "var1", "var2", "var3", "var4", "var5")

    val columnsToSum = List(col("var1"), col("var2"), col("var3"), col("var4"), col("var5"))

    val output = input.withColumn("sums", columnsToSum.reduce(_ + _))
    
    val a = when($"var1"<5,"Y").otherwise("N").as("a")
    val b = (columnsToSum.reduce(_  + _ ) / 60).as("b")
    val k = input.select(a,b)
    val c = input.groupBy($"var1", $"var4").agg(round(avg("var2")).as("var2"),round(avg("var3")).as("var3"))
    c.show()
//    
    val x = Array[Int](1,2,3)
    val y = Array[Int](1,2,3)
    
    val df2 =  new DecimalFormat("#.#");
    val d=  df2.format(22111111.40222).toDouble
    println(d.toString())
    
    println(x.sameElements(y))
  }
}