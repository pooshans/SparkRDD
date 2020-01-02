package spark.extend.rdd.example;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import spark.extend.rdd.FeeConcessionRDD2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author PooshanSingh
 */
public class SparkExtendExample2 {
    public static void main(String[] args) {

        List<Tuple2<String,Double>> dataSourcesDiscountTuple = new ArrayList<>();
        dataSourcesDiscountTuple.add(new Tuple2<>("StudentRecords1",0.1));
        dataSourcesDiscountTuple.add(new Tuple2<>("StudentRecords2",0.2));

        JavaSparkContext sc = new JavaSparkContext("local[*]", "extendingspark");

        FeeConcessionRDD2 feeConcessionRDD2 = new FeeConcessionRDD2(sc, dataSourcesDiscountTuple);

        System.out.println("Total count : " + feeConcessionRDD2.toJavaRDD().collect().size());

        System.out.println(feeConcessionRDD2.toJavaRDD().collect());
    }
}
