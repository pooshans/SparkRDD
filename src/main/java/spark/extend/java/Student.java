package spark.extend.java;

import java.io.Serializable;

/**
 * @author PooshanSingh
 */
public class Student implements Comparable<Student>,Serializable {
    private String studentName;
    private String rollNumber;
    private int semester;
    private int category;
    private double semesterFee;


    public Student(String studentName, String rollNumber, int semester,int category, double semesterFee) {
        this.studentName = studentName;
        this.rollNumber = rollNumber;
        this.semester = semester;
        this.category = category;
        this.semesterFee = semesterFee;
    }

    public int compareTo(Student o) {
        return this.studentName.compareTo(o.studentName);
    }

    @Override
    public String toString() {
        return "\nStudent{" +
                "studentName='" + studentName + '\'' +
                ", rollNumber='" + rollNumber + '\'' +
                ", semester=" + semester +
                ", category=" + category +
                ", semesterFee=" + semesterFee +
                '}'+
                "\n";
    }

    public String getStudentName() {
        return studentName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getRollNumber() {
        return rollNumber;
    }

    public void setRollNumber(String rollNumber) {
        this.rollNumber = rollNumber;
    }

    public int getSemester() {
        return semester;
    }

    public void setSemester(int semester) {
        this.semester = semester;
    }

    public double getSemesterFee() {
        return semesterFee;
    }

    public void setSemesterFee(double semesterFee) {
        this.semesterFee = semesterFee;
    }

    public int getCategory() {
        return category;
    }

    public void setCategory(int category) {
        this.category = category;
    }
}
