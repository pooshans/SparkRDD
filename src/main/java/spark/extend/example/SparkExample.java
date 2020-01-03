package spark.extend.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author PooshanSingh
 */
public class SparkExample {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext("local[*]", "spark");
        JavaRDD<String> javaRDD1 =  sc.textFile("src/main/resources/StudentRecords1");
        String header1 =  javaRDD1.first();
        System.out.println(header1);
        JavaRDD<String> javaRDD2 =  sc.textFile("src/main/resources/StudentRecords2");
        String header2 = javaRDD2.first();
        JavaRDD<String> javaRDD3 = javaRDD1.filter(line-> !line.equalsIgnoreCase(header1)).union(javaRDD2.filter(line->!line.equalsIgnoreCase(header2)));
        JavaRDD javaRDD4 = javaRDD3.cache().map(lines->Arrays.asList(new Object[]{ lines.split(",")[0],lines.split(",")[1],lines.split(",")[2],lines.split(",")[3],Double.valueOf(lines.split(",")[4]) * 0.1}));
        //javaRDD4.toLocalIterator().forEachRemaining(System.out::println);
        System.out.println(javaRDD4.collect());
        sc.stop();


    }
}
