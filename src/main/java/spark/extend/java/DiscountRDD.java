package spark.extend.java;

import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
    private String dataSource;

    private SparkContext sc;
    public DiscountRDD(JavaRDD<SalesRecord> salesRecordRDD, double discountPercentage) {
        super(salesRecordRDD.rdd(),SALES_RECORD_CLASS_TAG_TAG);
        this.discountPercentage = discountPercentage;
        this.salesRecordRDD = salesRecordRDD;
    }

    public DiscountRDD(JavaSparkContext sc, String dataSource, double discountPercentage) {
        super(sc.sc(),new ArrayBuffer<>(),SALES_RECORD_CLASS_TAG_TAG);
        this.discountPercentage = discountPercentage;
        this.salesRecordRDD = salesRecordRDD;
        this.dataSource = dataSource;
    }


    @Override
    public Iterator<SalesRecord> compute(Partition split, TaskContext context) {
        DiscountPartition  discountPartition = (DiscountPartition)split;
        return new DiscountIterator(discountPartition);
    }

    public Partition[] getPartitions() {
        List<DiscountPartition> discountPartitions = new ArrayList<DiscountPartition>();
        int index = 0;
        BufferedReader objReader = null;
        try {
            String strCurrentLine;

            objReader = new BufferedReader(new FileReader(dataSource));
            List<SalesRecord> salesRecords = null;
            int rowCount = 0;

            while ((strCurrentLine = objReader.readLine()) != null) {
                if(salesRecords == null){
                    salesRecords = new ArrayList<>();
                }


                String[] colValues = strCurrentLine.split(",");
                SalesRecord salesRecord =  new SalesRecord(colValues[0], colValues[1], colValues[2], Double.valueOf(colValues[3]));
                salesRecords.add(salesRecord);
                rowCount++;
                if(rowCount%2 == 0){
                    //Each partition will have two row.
                    DiscountPartition discountPartition = new DiscountPartition(index,salesRecords.toArray(new SalesRecord[]{}));
                    discountPartitions.add(discountPartition);
                    index++;
                    salesRecords = null;
                }
            }
            return discountPartitions.toArray(new DiscountPartition[]{});

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try {
                if (objReader != null)
                    objReader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

return null;
    }


    /**
     * Each partition definition
     **/
    public static class DiscountPartition implements Partition {
        private static final long serialVersionUID = 1L;
        private int index;
        private SalesRecord[] salesRecords;

        public DiscountPartition(int index,SalesRecord[] salesRecords) {
            this.index = index;
            this.salesRecords = salesRecords;
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
     * Iterators over all SalesRecords
     */
    public static class DiscountIterator extends AbstractIterator<SalesRecord> {
        private SalesRecord[] salesRecords;
        private int rowIndex = 0;


        public DiscountIterator(DiscountPartition discountPartition) {
            this.salesRecords = discountPartition.salesRecords;

        }

        @Override
        public boolean hasNext() {
            return rowIndex < salesRecords.length;
        }

        @Override
        public SalesRecord next() {
            double discount = salesRecords[rowIndex].getItemValue()*discountPercentage;
            SalesRecord newSalesRecord =  new SalesRecord(salesRecords[rowIndex].getTransactionId(),salesRecords[rowIndex].getCustomerId(),salesRecords[rowIndex].getItemId(),discount);
            rowIndex++;
            return  newSalesRecord;

        }
    }
}


