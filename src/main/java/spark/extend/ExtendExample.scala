package spark.extend

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import CustomFunctions._

/**
  * @author PooshanSingh
  */
object ExtendExample {
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "extendingspark")
    val dataRDD = sc.textFile("/Users/pooshans/research/code/blog/SparkRDD/src/main/resources/sales.csv")
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),colValues(2),colValues(3).toDouble)
    })

    println(salesRecordRDD.totalSales)

    // discount RDD
    val discountRDD = salesRecordRDD.discount(0.1)
    println(discountRDD.collect().toList)
  }
}
