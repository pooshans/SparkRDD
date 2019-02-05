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
public class FeeConcessionRDD extends RDD<Student> {

    protected static  double discountPercentage;
    private RDD<Student> prev;
    private static final ClassTag<Student> SALES_RECORD_CLASS_TAG_TAG = ClassManifestFactory$.MODULE$.fromClass(Student.class);
    private JavaRDD<Student> salesRecordRDD;
    private List<String> dataSources;

    public FeeConcessionRDD(JavaSparkContext sc, List<String> dataSources, double discountPercentage) {
        super(sc.sc(),new ArrayBuffer<>(),SALES_RECORD_CLASS_TAG_TAG);
        this.discountPercentage = discountPercentage;
        this.salesRecordRDD = salesRecordRDD;
        this.dataSources = dataSources;
    }


    @Override
    public Iterator<Student> compute(Partition split, TaskContext context) {
        FeeConcessionPartition feeConcessionPartition = (FeeConcessionPartition)split;
        return new FeeConcessionIterator(feeConcessionPartition);
    }

    public Partition[] getPartitions() {
        return makePartitions();

    }

    private Partition[] makePartitions() {
        List<FeeConcessionPartition> feeConcessionPartitions = new ArrayList<>();
        int index = 0;
        for(String dataSource : dataSources) {
            FeeConcessionPartition feeConcessionPartition = new FeeConcessionPartition(id(), index++,"src/main/resources/"+dataSource,discountPercentage);
            feeConcessionPartitions.add(feeConcessionPartition);
        }
        return feeConcessionPartitions.toArray(new FeeConcessionPartition[]{});
    }


}


