package net.sansa_stack.ml.spark.explainableanomalydetection2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class SyntheticDataGeneratorOriginal {

    private static final int numberOfRows = 50000000;
    private static final int WRITE_THRESHOLD=1000000-1;
    private static  final String RDF_FILE = "/home/farshad/Desktop/ExAd8April2022/synthetic/data_" + numberOfRows + ".ttl";
    private static  final String CSV_FILE = "/home/farshad/Desktop/ExAd8April2022/synthetic/data_" + numberOfRows + ".csv";
    public static void main(String[] args) throws IOException {
        final float studentPercentage = 0.50f;
        Files.createFile(Paths.get(RDF_FILE));
        Files.createFile(Paths.get(CSV_FILE));
        new SyntheticDataGeneratorOriginal().run(studentPercentage,numberOfRows);
    }

    public void run(float studentPercentage, int  numberOfRows){
        String prefixRDF ="@prefix : <http://dig.isi.edu/> .\n" +
                "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
                "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n" +
                "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .";

        String prefixCSV="id,age,gender,pregnant,job";

        StringBuilder finalResultRDF = new StringBuilder();
        StringBuilder finalResultCSV = new StringBuilder();
//        finalResultRDF.append(prefixRDF).append("\n");
        finalResultCSV.append(prefixCSV).append("\n");
        int numberOfStudents = (int)(numberOfRows*studentPercentage);
        for(int i=0;i<numberOfRows;i++){
            if(i<numberOfStudents){
                finalResultRDF.append(generateStudent(i).toNTripleRDF());
                finalResultCSV.append(generateStudent(i));
            }else{
                finalResultRDF.append(generatePresident(i).toNTripleRDF());
                finalResultCSV.append(generatePresident(i));
            }
            if(i%WRITE_THRESHOLD==0){
                System.out.println("chunk "+i);
                write(finalResultRDF,finalResultCSV);
                finalResultRDF = new StringBuilder();
                finalResultCSV = new StringBuilder();
            }
        }
//        write(numberOfRows,finalResultRDF,finalResultCSV);
        finalResultRDF = new StringBuilder();
        finalResultCSV = new StringBuilder();
        addAnomaly(finalResultRDF,finalResultCSV,numberOfRows);
        write(finalResultRDF,finalResultCSV);


    }

    private void write(StringBuilder finalResultRDF, StringBuilder finalResultCSV) {
        try {
            Path pathRDF = Paths.get(RDF_FILE);
            byte[] strToBytesRDF = finalResultRDF.toString().getBytes();
            Files.write(pathRDF, strToBytesRDF, StandardOpenOption.APPEND);

//            Path pathCSV = Paths.get(CSV_FILE);
//            byte[] strToBytesCSV = finalResultCSV.toString().getBytes();
//            Files.write(pathCSV, strToBytesCSV,StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addAnomaly(StringBuilder finalResultRDF, StringBuilder finalResultCSV,int id) {
        for(int i=0;i<2;i++){
        Person p1 = new Person();
        p1.setId(id+i*4);
        p1.setSex(false);
        p1.setAge(90);
        p1.setPregnant(true);
        p1.setJob("President");
        finalResultRDF.append(p1.toNTripleRDF());
        finalResultCSV.append(p1);

        Person p2 = new Person();
        p2.setId(id+1+i*4);
        p2.setSex(true);
        p2.setAge(2);
        p2.setPregnant(true);
        p2.setJob("President");
        finalResultRDF.append(p2.toNTripleRDF());
        finalResultCSV.append(p2);

        p2 = new Person();
        p2.setId(id+2+i*4);
        p2.setSex(true);
        p2.setAge(2);
        p2.setPregnant(false);
        p2.setJob("President");
        finalResultRDF.append(p2.toNTripleRDF());
        finalResultCSV.append(p2);

        p2 = new Person();
        p2.setId(id+3+i*4);
        p2.setSex(false);
        p2.setAge(100);
        p2.setPregnant(false);
        p2.setJob("Student");
        finalResultRDF.append(p2.toNTripleRDF());
        finalResultCSV.append(p2);}

    }

    private Person generatePresident(int id) {
        Person p = new Person();
        p.setId(id);
        int min = 25;
        int max = 70;
        p.setAge(getAge(min,max));
        p.setSex(getSex(0.9f));
        p.setJob("President");
        p.setPregnant(getPresidentPregnant(p.isSex(),p.getAge()));
        return p;
    }

    private boolean getPresidentPregnant(boolean sex,int age) {
        if(sex==false) {
            return false;
        }else{
            if(age>55){
                return false;
            }
            else{
                if(Math.random()<0.4){
                    return true;
                }else{
                    return false;
                }
            }
        }

    }

    private boolean getSex(float maleThreshold) {
        if(Math.random()<maleThreshold)
            return true;
        else
            return false;
    }

    private Person generateStudent(int id) {
        Person p = new Person();
        p.setId(id);
        int min = 7;
        int max = 14;
        p.setAge(getAge(min,max));
        p.setSex(getSex(0.5f));
        p.setJob("Student");
        p.setPregnant(false);
        return p;
    }

//    private int getStudentHeight(int age) {
//        return age+
//    }



    private int getAge(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }



    class Person {
        private int id;
        private int age;
//        private int height;
        private boolean sex;
        private boolean pregnant;
        private String job;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

//        public int getHeight() {
//            return height;
//        }
//
//        public void setHeight(int height) {
//            this.height = height;
//        }

        public boolean isSex() {
            return sex;
        }

        public void setSex(boolean sex) {
            this.sex = sex;
        }

        public boolean isPregnant() {
            return pregnant;
        }

        public void setPregnant(boolean pregnant) {
            this.pregnant = pregnant;
        }

        public String getJob() {
            return job;
        }

        public void setJob(String job) {
            this.job = job;
        }

        @Override
        public String toString() {
            return id +","+ age +","+ sex +","+ pregnant +","+ job+"\n";
        }

        public String toRDF(){
            StringBuilder result = new StringBuilder();
            result.append(":Person" + id + " :id " + id + ";").append("\n");
            result.append("\t:age " + age + ";").append("\n");
            result.append("\t:gender " + sex + ";").append("\n");
            result.append("\t:pregnant " + pregnant + ";").append("\n");
            result.append("\t:job :" + job + ".").append("\n\n");
            return result.toString();
        }

        public String toNTripleRDF(){
            StringBuilder result = new StringBuilder();
            result.append("<http://dig.isi.edu/Person"+id+"> " + "<http://dig.isi.edu/id> " + "\""+id+"\"^^<http://www.w3.org/2001/XMLSchema#integer> .").append("\n");
            result.append("<http://dig.isi.edu/Person"+id+"> " + "<http://dig.isi.edu/age> " + "\""+age+"\"^^<http://www.w3.org/2001/XMLSchema#integer> .").append("\n");
            result.append("<http://dig.isi.edu/Person"+id+"> " + "<http://dig.isi.edu/sex> " + "\""+sex+"\"^^<http://www.w3.org/2001/XMLSchema#boolean> .").append("\n");
            result.append("<http://dig.isi.edu/Person"+id+"> " + "<http://dig.isi.edu/pregnant> " + "\""+pregnant+"\"^^<http://www.w3.org/2001/XMLSchema#boolean> .").append("\n");
            result.append("<http://dig.isi.edu/Person"+id+"> " + "<http://dig.isi.edu/job> " + "<http://dig.isi.edu/"+job+"> .").append("\n");
            return result.toString();
        }
    }
}
