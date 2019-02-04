package spark.extend.java;

import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author PooshanSingh
 */
public class SparkExtendExample {
    public static void main(String[] args) {

        List<String> dataSources = new ArrayList<>();
        dataSources.add("sales1");
        dataSources.add("sales2");

        JavaSparkContext sc = new JavaSparkContext("local[*]", "extendingspark");

        DiscountRDD discountRDD = new DiscountRDD(sc, dataSources,0.1);

        System.out.println("Total count : " + discountRDD.toJavaRDD().collect().size());

        discountRDD.toJavaRDD().collect().forEach(e->{
            System.out.println(e.getTransactionId()+","+e.getCustomerId()+","+e.getItemId()+","+e.getItemValue());
        });
    }
}
