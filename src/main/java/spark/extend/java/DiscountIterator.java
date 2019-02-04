package spark.extend.java;

import scala.collection.AbstractIterator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Iterators over all SalesRecords
 */
public class DiscountIterator extends AbstractIterator<SalesRecord> {
    private DiscountPartition discountPartition;
    private int rowIndex = 0;
    BufferedReader objReader;
    boolean isFirstLine = true;
    String strCurrentLine = null;



    public DiscountIterator(DiscountPartition discountPartition) {
        this.discountPartition = discountPartition;
        try {
            objReader = new BufferedReader(new FileReader(discountPartition.dataSource));
            objReader.readLine(); // skip the header
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean hasNext() {
        try {
            strCurrentLine = objReader.readLine();
            return strCurrentLine != null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public SalesRecord next() {
                String[] colValues = strCurrentLine.split(",");
                SalesRecord salesRecord =  new SalesRecord(colValues[0], colValues[1], colValues[2], Double.valueOf(colValues[3])*discountPartition.discount);
                return salesRecord;

    }
}
