package spark.extend.java;

import org.apache.spark.Partition;

/**
 * Each partition definition. It consist of set of student records.
 **/
public class FeeConcessionPartition implements Partition {
    private static final long serialVersionUID = 1L;
    private int index;
    private int rddId;
    protected String dataSource;
    protected double feeConcessionPercentage;

    public FeeConcessionPartition(int rddId, int index, String dataSource, double feeConcessionPercentage) {
        this.index = index;
        this.dataSource = dataSource;
        this.rddId = rddId;
        this.feeConcessionPercentage = feeConcessionPercentage;
    }

    @Override
    public int index() {
        return index;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof FeeConcessionPartition)) {
            return false;
        }
        return ((FeeConcessionPartition)obj).index != index;
    }

    @Override
    public int hashCode() {
        return 41 * (41 + rddId) + index();
    }
}
