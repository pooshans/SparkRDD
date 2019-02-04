package spark.extend.java;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;

/**
 * @author PooshanSingh
 */
public class DiscountRDD extends RDD<SalesRecord> {

    protected static  double discountPercentage;
    private RDD<SalesRecord> prev;
    private static final ClassTag<SalesRecord> SALES_RECORD_CLASS_TAG_TAG = ClassManifestFactory$.MODULE$.fromClass(SalesRecord.class);
    private JavaRDD<SalesRecord> salesRecordRDD;
    private List<String> dataSources;

    public DiscountRDD(JavaSparkContext sc, List<String> dataSources, double discountPercentage) {
        super(sc.sc(),new ArrayBuffer<>(),SALES_RECORD_CLASS_TAG_TAG);
        this.discountPercentage = discountPercentage;
        this.salesRecordRDD = salesRecordRDD;
        this.dataSources = dataSources;
    }


    @Override
    public Iterator<SalesRecord> compute(Partition split, TaskContext context) {
        DiscountPartition  discountPartition = (DiscountPartition)split;
        return new DiscountIterator(discountPartition);
    }

    public Partition[] getPartitions() {
        return makePartitions();

    }

    private Partition[] makePartitions() {
        List<DiscountPartition> discountPartitions = new ArrayList<>();
        int index = 0;
        for(String dataSource : dataSources) {
            DiscountPartition discountPartition = new DiscountPartition(id(), index++,"src/main/resources/"+dataSource,discountPercentage);
            discountPartitions.add(discountPartition);
        }
        return discountPartitions.toArray(new DiscountPartition[]{});
    }


}


