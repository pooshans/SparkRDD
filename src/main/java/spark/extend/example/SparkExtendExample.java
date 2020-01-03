package spark.extend.example;

import org.apache.spark.api.java.JavaSparkContext;
import spark.extend.rdd.FeeConcessionRDD;

import java.util.ArrayList;
import java.util.List;

/**
 * @author PooshanSingh
 */
public class SparkExtendExample {
    public static void main(String[] args) {

        List<String> dataSources = new ArrayList<>();
        dataSources.add("StudentRecords1");
        dataSources.add("StudentRecords2");

        JavaSparkContext sc = new JavaSparkContext("local[*]", "extendingspark");

        FeeConcessionRDD feeConcessionRDD = new FeeConcessionRDD(sc, dataSources,0.1);

        System.out.println("Total count : " + feeConcessionRDD.toJavaRDD().collect().size());

        System.out.println(feeConcessionRDD.toJavaRDD().collect());
        sc.stop();
    }
}
