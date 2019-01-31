package spark.extend.java;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author PooshanSingh
 */
public class SparkExtendExample {
    public static void main(String[] args) {

        String dataSource = "sales.csv";


        JavaSparkContext sc = new JavaSparkContext("local[*]", "extendingspark");
        JavaRDD<String> dataRdd = sc.textFile("src/main/resources/"+dataSource,
                5);
        JavaRDD<SalesRecord> salesRecordRDD = dataRdd.map(row -> {
            String[] colValues = row.split(",");
            return new SalesRecord(colValues[0], colValues[1], colValues[2], Double.valueOf(colValues[3]));
        });

        //DiscountRDD discountRDD = new DiscountRDD(salesRecordRDD,0.1);


        DiscountRDD discountRDD = new DiscountRDD(sc, dataSource,0.1);

        System.out.println("Total count : " + discountRDD.toJavaRDD().collect().size());

        discountRDD.toJavaRDD().collect().forEach(e->{
            System.out.println(e.getTransactionId()+","+e.getCustomerId()+","+e.getItemId()+","+e.getItemValue());
        });
    }
}
