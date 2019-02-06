package spark.extend.scala

import org.apache.spark.SparkContext
import CustomFunctions._

/**
  * @author PooshanSingh
  */
object ExtendExample {
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "extendingspark")
    val dataRDD = sc.textFile("/Users/pooshans/research/code/blog/SparkRDD/src/main/resources/StudentRecords1")
    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),colValues(2),colValues(3).toDouble)
    })

    println("Total Sales : "+salesRecordRDD.totalSales)

    // feeConcessionPercentage RDD
    val discountRDD = salesRecordRDD.discount(0.1) // 10% feeConcessionPercentage.

    discountRDD.collect().foreach({
      println("Txn Id,Cust Id,Item Id,Item Value");
      println(_);
    });
  }
}
