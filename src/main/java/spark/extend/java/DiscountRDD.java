package spark.extend.java;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.reflect.ClassTag;

/**
 * @author PooshanSingh
 */
public class DiscountRDD extends RDD<SalesRecord> {

    private double discountPercentage;
    private RDD<SalesRecord> prev;
    private ClassTag<SalesRecord> evidence$2;

    public DiscountRDD(JavaRDD<SalesRecord> oneParent, ClassTag<SalesRecord> evidence$2, double discountPercentage) {
        super(oneParent.rdd(), evidence$2);
        this.discountPercentage = discountPercentage;
    }


    public Iterator<SalesRecord> compute(Partition split, TaskContext context) {
        return firstParent(super.elementClassTag()).toJavaRDD().map(salesRecord->{
            double discount = salesRecord.getItemValue()*discountPercentage;
            return new SalesRecord(salesRecord.getTransactionId(),salesRecord.getCustomerId(),salesRecord.getItemId(),discount);
        }).rdd().iterator(split, context);
    }

    public Partition[] getPartitions() {
        return new Partition[0];
    }
}
