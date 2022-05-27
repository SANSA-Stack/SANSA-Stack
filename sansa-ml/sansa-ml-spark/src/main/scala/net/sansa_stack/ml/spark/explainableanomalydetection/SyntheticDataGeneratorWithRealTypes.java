package net.sansa_stack.ml.spark.explainableanomalydetection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class SyntheticDataGeneratorWithRealTypes {


    public static void main(String[] args) throws IOException {
        args = new String[]{"20000", "/home/farshad/Desktop/ExAd8April2022/synthetic/data3_20k.ttl","/home/farshad/Desktop/ExAd8April2022/synthetic/data3_20k.csv"};
        final float studentPercentage = 0.50f;
        final int numberOfRows = Integer.parseInt(args[0]);
        final int WRITE_THRESHOLD = 1000000 - 1;
        final String RDF_FILE = args[1];//  "/home/farshad/Desktop/ExAd8April2022/synthetic/data_" + numberOfRows + ".ttl";
        final String CSV_FILE = args[2];//"/home/farshad/Desktop/ExAd8April2022/synthetic/data_" + numberOfRows + ".csv";

        Files.createFile(Paths.get(RDF_FILE));
        Files.createFile(Paths.get(CSV_FILE));
        new SyntheticDataGeneratorWithRealTypes().run(studentPercentage, numberOfRows, WRITE_THRESHOLD, RDF_FILE,CSV_FILE);
    }

    public void run(float studentPercentage, int numberOfRows, int WRITE_THRESHOLD, String RDF_FILE,String CSV_FILE) {
        String prefixCSV = "id,age,sex,pregnant,job";

        StringBuilder finalResultRDF = new StringBuilder();
        StringBuilder finalResultCSV = new StringBuilder();
        finalResultCSV.append(prefixCSV).append("\n");
        int numberOfStudents = (int) (numberOfRows * studentPercentage);
        for (int i = 0; i < numberOfRows; i++) {
            if (i < numberOfStudents) {
                finalResultRDF.append(generateStudent(i).toNTripleRDFOriginal());
                finalResultCSV.append(generateStudent(i));
            } else {
                finalResultRDF.append(generatePresident(i).toNTripleRDFOriginal());
                finalResultCSV.append(generatePresident(i));
            }

        }
//        if (i % WRITE_THRESHOLD == 0) {
//            System.out.println("chunk " + i);
            write(finalResultRDF, finalResultCSV, RDF_FILE,CSV_FILE);
//            finalResultRDF = new StringBuilder();
//            finalResultCSV = new StringBuilder();
//        }

//        write(numberOfRows,finalResultRDF,finalResultCSV);
        finalResultRDF = new StringBuilder();
        finalResultCSV = new StringBuilder();
        addAnomaly(finalResultRDF, finalResultCSV, numberOfRows);
        write(finalResultRDF, finalResultCSV, RDF_FILE,CSV_FILE);


    }

    private void write(StringBuilder finalResultRDF, StringBuilder finalResultCSV, String RDF_FILE,String CSV_FILE) {
        try {
            Path pathRDF = Paths.get(RDF_FILE);
            byte[] strToBytesRDF = finalResultRDF.toString().getBytes();
            Files.write(pathRDF, strToBytesRDF, StandardOpenOption.APPEND);

            Path pathCSV = Paths.get(CSV_FILE);
            byte[] strToBytesCSV = finalResultCSV.toString().getBytes();
            Files.write(pathCSV, strToBytesCSV,StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addAnomaly(StringBuilder finalResultRDF, StringBuilder finalResultCSV, int id) {
        int totalNumberOfAnomalies = 20;
        int totalNumberOfPregnant = totalNumberOfAnomalies/2;//10;
        int totalNumberOfPresident = totalNumberOfAnomalies/4;
        int totalNumberOfStudent = totalNumberOfAnomalies/4;

        for(int i=1;i<=totalNumberOfPregnant;i++){
            Person p = new Person();
            p.setId(id + i);
            p.setSex(false);
            p.setAge(getAge(90,100));
            p.setPregnant(true);
            p.setJob("President");
            finalResultRDF.append(p.toNTripleRDFOriginal());
            finalResultCSV.append(p);
        }

        for(int i=1;i<=totalNumberOfPresident;i++){
            Person p = new Person();
            p.setId(id + totalNumberOfPregnant+i);
            p.setSex(false);
            p.setAge(getAge(4,10));
            p.setPregnant(false);
            p.setJob("President");
            finalResultRDF.append(p.toNTripleRDFOriginal());
            finalResultCSV.append(p);
        }

        for(int i=1;i<=totalNumberOfStudent;i++){
            Person p = new Person();
            p.setId(id + totalNumberOfPregnant+totalNumberOfPresident+i);
            p.setSex(false);
            p.setAge(getAge(70,90));
            p.setPregnant(false);
            p.setJob("Student");
            finalResultRDF.append(p.toNTripleRDFOriginal());
            finalResultCSV.append(p);
        }
    }

    private Person generatePresident(int id) {
        Person p = new Person();
        p.setId(id);
        int min = 25;
        int max = 70;
        p.setAge(getAge(min, max));
        p.setSex(getSex(0.9f));
        p.setJob("President");
        p.setPregnant(getPresidentPregnant(p.sex, p.getAge()));
        return p;
    }

    private boolean getPresidentPregnant(boolean sex, int age) {
        if (sex == false) {
            return false;
        } else {
            if (age > 55) {
                return false;
            } else {
                if (Math.random() < 0.4) {
                    return true;
                } else {
                    return false;
                }
            }
        }

    }

    private boolean getSex(float maleThreshold) {
        if (Math.random() < maleThreshold)
            return true;
        else
            return false;
    }

    private Person generateStudent(int id) {
        Person p = new Person();
        p.setId(id);
        int min = 7;
        int max = 14;
        p.setAge(getAge(min, max));
        p.setSex(getSex(0.5f));
        p.setJob("Student");
        p.setPregnant(false);
        return p;
    }

    private int getAge(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    class Person {
        private int id;
        private int age;
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
            return id + "," + age + "," + sex + "," + pregnant + "," + job + "\n";
        }

        public String toRDF() {
            StringBuilder result = new StringBuilder();
            result.append(":Person" + id + " :id " + id + ";").append("\n");
            result.append("\t:age " + age + ";").append("\n");
            result.append("\t:gender " + sex + ";").append("\n");
            result.append("\t:pregnant " + pregnant + ";").append("\n");
            result.append("\t:job :" + job + ".").append("\n\n");
            return result.toString();
        }

        public String toNTripleRDFOriginal() {
            StringBuilder result = new StringBuilder();
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/id> " + "\"" + id + "\"^^<http://www.w3.org/2001/XMLSchema#integer> .").append("\n");
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/age> " + "\"" + age + "\"^^<http://www.w3.org/2001/XMLSchema#integer> .").append("\n");
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/sex> " + "\"" + sex + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> .").append("\n");
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/pregnant> " + "\"" + pregnant + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> .").append("\n");
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/job> " + "<http://dig.isi.edu/" + job + "> .").append("\n");
            return result.toString();
        }

        public String toNTripleRDF() {
            StringBuilder result = new StringBuilder();
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/id> " + id + " .").append("\n");
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/age> " + age + " .").append("\n");
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/sex> " + sex + " .").append("\n");
            result.append("<http://dig.isi.edu/Person" + id + "> " + "<http://dig.isi.edu/pregnant> " + pregnant + " .").append("\n");
            result.append("<http://dig.isi.edu/Person"+id+"> " + "<http://dig.isi.edu/job> " + "<http://dig.isi.edu/"+job+"> .").append("\n");
            return result.toString();
        }
    }
}
