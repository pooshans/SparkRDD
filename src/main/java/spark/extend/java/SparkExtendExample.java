package spark.extend.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author PooshanSingh
 */
public class SparkExtendExample {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local[*]", "extendingspark");
        JavaRDD<String> dataRdd = sc.textFile("/Users/pooshans/research/code/blog/SparkRDD/src/main/resources/sales.csv",
                5);
        JavaRDD<SalesRecord> salesRecordRDD = dataRdd.map(row -> {
            String[] colValues = row.split(",");
            return new SalesRecord(colValues[0], colValues[1], colValues[2], Double.valueOf(colValues[3]));
        });

        //double totalSales = salesRecordRDD.mapToDouble((DoubleFunction<SalesRecord>) salesRecord -> salesRecord.getItemValue()).sum();

        //System.out.println("Total Sales : "+totalSales);

        DiscountRDD discountRDD = new DiscountRDD(salesRecordRDD, 0.1);

        System.out.println("Total count : " + discountRDD.toJavaRDD().collect().size());

        discountRDD.toJavaRDD().collect().forEach(e->{
            System.out.println(e.getTransactionId()+","+e.getCustomerId()+","+e.getItemId()+","+e.getItemValue());
        });
    }
}
