package spark.extend.rdd;

import scala.collection.AbstractIterator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Iterators over all StudentsRecords which is part of partition {@link spark.extend.rdd.FeeConcessionPartition}
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
       String studentName = (colValues[0]!= null) ? colValues[0].trim() : "";
       String rollNumber = (colValues[1] != null) ? colValues[1].trim() : "";
       int semester = (colValues[2] != null ) ? Integer.valueOf(colValues[2].trim()): -1;
       int categoryCode = (colValues[3] != null) ? Integer.valueOf(colValues[3].trim()) : -1;
       double fee = (colValues[4] != null) ? Double.valueOf(colValues[4].trim()) : 0.0;

       Student student =  new Student(studentName, rollNumber, semester,categoryCode,fee*feeConcessionPartition.feeConcessionPercentage);
       return student;
    }
}
