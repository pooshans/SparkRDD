package spark.extend.java;

import java.io.Serializable;

/**
 * @author PooshanSingh
 */
public class SalesRecord implements Comparable<SalesRecord>,Serializable {
    private String transactionId;
    private String customerId;
    private String itemId;
    private double itemValue;


    public SalesRecord(String transactionId, String customerId, String itemId, double itemValue) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.itemId = itemId;
        this.itemValue = itemValue;
    }

    public int compareTo(SalesRecord o) {
        return this.transactionId.compareTo(o.transactionId);
    }

    @Override
    public String toString() {
        return "SalesRecord{" +
                "transactionId='" + transactionId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", itemValue=" + itemValue +
                '}';
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public double getItemValue() {
        return itemValue;
    }

    public void setItemValue(double itemValue) {
        this.itemValue = itemValue;
    }
}
