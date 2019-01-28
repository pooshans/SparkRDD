package spark.extend.java;

import org.apache.spark.Partition;

/**
 * Each partition definition
 **/
public class DiscountPartition implements Partition {
    private static final long serialVersionUID = 1L;
    private int index;
    protected SalesRecord[] salesRecords;

    public DiscountPartition(int index, SalesRecord[] salesRecords) {
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
