package spark.extend.java;

import scala.collection.AbstractIterator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Iterators over all SalesRecords
 */
public class FeeConcessionIterator extends AbstractIterator<Student> {
    private FeeConcessionPartition feeConcessionPartition;
    private int rowIndex = 0;
    BufferedReader objReader;
    boolean isFirstLine = true;
    String strCurrentLine = null;



    public FeeConcessionIterator(FeeConcessionPartition feeConcessionPartition) {
        this.feeConcessionPartition = feeConcessionPartition;
        try {
            objReader = new BufferedReader(new FileReader(feeConcessionPartition.dataSource));
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
    public Student next() {
        //Student Name,Roll Number,Semester,Category,Fee
        //ABC         ,001        ,1       ,5   ,50000.0

                String[] colValues = strCurrentLine.split(",");
                Student student =  new Student(colValues[0].trim(), colValues[1].trim(), Integer.valueOf(colValues[2].trim()),Integer.valueOf(colValues[3].trim()),Double.valueOf(colValues[4].trim())*feeConcessionPartition.feeConcessionPercentage);
                return student;
    }
}
