package spark.extend.java;

import scala.collection.AbstractIterator;

/**
 * Iterators over all SalesRecords
 */
public class DiscountIterator extends AbstractIterator<SalesRecord> {
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
        double discount = salesRecords[rowIndex].getItemValue()* DiscountRDD.discountPercentage;
        SalesRecord newSalesRecord =  new SalesRecord(salesRecords[rowIndex].getTransactionId(),salesRecords[rowIndex].getCustomerId(),salesRecords[rowIndex].getItemId(),discount);
        rowIndex++;
        return  newSalesRecord;

    }
}
