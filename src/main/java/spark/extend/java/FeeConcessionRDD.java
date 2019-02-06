package spark.extend.java;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;

/**
 * This class just extend the RDD of Spark and implements it's extended developer's APIs.
 * @author PooshanSingh
 */
public class FeeConcessionRDD extends RDD<Student> {

    protected static  double discountPercentage;
    // This TYPE must be the one which will be formed for RDD.
    private static final ClassTag<Student> STUDENT_RECORD_CLASS_TAG_TAG = ClassManifestFactory$.MODULE$.fromClass(Student.class);

    //This is the datasource to read the block of files/ records.
    private List<String> dataSources;

    public FeeConcessionRDD(JavaSparkContext sc, List<String> dataSources, double discountPercentage) {
        super(sc.sc(),new ArrayBuffer<>(), STUDENT_RECORD_CLASS_TAG_TAG);
        this.discountPercentage = discountPercentage;
        this.dataSources = dataSources;
    }


    /**
     *
     * @param split Computation is done for each partition.
     * @param context It's the task context.
     * @return Iterator over each partition.
     */
    @Override
    public Iterator<Student> compute(Partition split, TaskContext context) {
        FeeConcessionPartition feeConcessionPartition = (FeeConcessionPartition)split;
        return new FeeConcessionIterator(feeConcessionPartition);
    }

    /**
     *
     * @return the list of partitions. Each partition can be logical set of records or say set of physical files.
     * Here, we have included the partitions considering the physical set of records.
     */
    public Partition[] getPartitions() {
        return makePartitions();

    }

    /**
     *
     * @return All possible partitions.
     */
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


