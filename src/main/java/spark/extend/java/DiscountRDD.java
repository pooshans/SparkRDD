package spark.extend.java;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.util.List;

/**
 * @author PooshanSingh
 */
public class DiscountRDD extends RDD<SalesRecord> {

    protected static  double discountPercentage;
    private RDD<SalesRecord> prev;
    private static final ClassTag<SalesRecord> SALES_RECORD_CLASS_TAG_TAG = ClassManifestFactory$.MODULE$.fromClass(SalesRecord.class);
    private JavaRDD<SalesRecord> salesRecordRDD;

    private SparkContext sc;
    public DiscountRDD(JavaRDD<SalesRecord> salesRecordRDD, double discountPercentage) {
        super(salesRecordRDD.rdd(),SALES_RECORD_CLASS_TAG_TAG);
        //super(sc.sc(),new ArrayBuffer<>(), SALES_RECORD_CLASS_TAG_TAG);
        this.discountPercentage = discountPercentage;
        this.salesRecordRDD = salesRecordRDD;
    }


    @Override
    public Iterator<SalesRecord> compute(Partition split, TaskContext context) {
        System.out.println("Split : " + split.index() + " , partition id : " + context.partitionId());
        DiscountPartition  discountPartition = (DiscountPartition)split;
        return new DiscountIterator(discountPartition.salesRecord);
    }

    public Partition[] getPartitions() {
        List<SalesRecord> salesRecords =  this.salesRecordRDD.collect();
        DiscountPartition[] discountPartitions = new DiscountPartition[salesRecords.size()];

        int index = 0;
        java.util.Iterator<SalesRecord> itr=  salesRecords.iterator();
        while(itr.hasNext()){
            discountPartitions[index] = new DiscountPartition(index,itr.next());
            index++;
        }
       return discountPartitions;

    }


    public static class DiscountPartition implements Partition {
        private static final long serialVersionUID = 1L;
        private int index;
        private SalesRecord salesRecord;

        public DiscountPartition(int index,SalesRecord salesRecord) {
            this.index = index;
            this.salesRecord = salesRecord;
        }

        @Override
        public int index() {
            return index;
        }

        @Override
        public boolean equals(Object obj) {
            if(!(obj instanceof DiscountPartition)) {
                return false;
            }
            return ((DiscountPartition)obj).index != index;
        }

        @Override
        public int hashCode() {
            return index();
        }
    }

    /**
     * Iterators over all characters between two characters
     */
    public static class DiscountIterator extends AbstractIterator<SalesRecord> {
        private SalesRecord salesRecord;
        private boolean isAvailble = true;


        public DiscountIterator(SalesRecord salesRecord) {
            this.salesRecord = salesRecord;

        }

        @Override
        public boolean hasNext() {
            return isAvailble;
        }

        @Override
        public SalesRecord next() {
            double discount = salesRecord.getItemValue()*discountPercentage;
            SalesRecord newSalesRecord =  new SalesRecord(salesRecord.getTransactionId(),salesRecord.getCustomerId(),salesRecord.getItemId(),discount);
            isAvailble = false;
            return  newSalesRecord;

        }
    }
}


