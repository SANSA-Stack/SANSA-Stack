/**
 * Created by mmami on 10.10.16.
 */
package net.sansa_stack.datalake.spark;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import net.sansa_stack.datalake.spark.model.Triple;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.Serializable;

public class NTtoDF implements Serializable {

    private String className;

    public NTtoDF() { }

    public NTtoDF options(Map<String, String> options) {
        className = options.get("class");

        return this;
    }

    //@SuppressWarnings("unchecked")
    public Dataset<Row> read(String input_path, SparkSession spark) {

        try {


            // 1. Read text file
            JavaRDD<String> lines = spark.read().textFile(input_path).toJavaRDD();
            //JavaRDD<String> lines = spark.read().textFile(input_path);

            // 2. Map lines to Triple objects
            JavaRDD<Triple> triples = lines.map((Function<String, Triple>) line -> {

                //String[] parts = line.split(" ");

                List<String> parts = new ArrayList<>();
                Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
                while (m.find())
                    parts.add(m.group(1));

                Triple triple;

                if (parts.get(1).equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))
                    triple = new Triple(replaceInValue(removeTagSymbol(parts.get(0))), null, replaceInValue(removeTagSymbol(parts.get(2))));
                else {
                    String subject = replaceInValue(removeTagSymbol(parts.get(0))); // MEASURE removeTagSymbol() time
                    String property = replaceInColumn(removeTagSymbol(parts.get(1)));
                    String object = replaceInValue(removeTagSymbol(parts.get(2)));
                    String type = replaceInValue(removeTagSymbol(parts.get(3))); // Either there is a type (xslt) or not (.)

                    String objectAndType = (parts.size() == 5) ? (object + type) : object;
                    objectAndType = reverse(objectAndType);

                    triple = new Triple(subject, property, objectAndType);
                }

                return triple;
            });


            // 3. Map Triple objects to pairs (Triple.subject,[Triple.property, Triple.object])
            //@SuppressWarnings({ "rawtypes" })
            JavaPairRDD<String, Tuple2<String, String>> subject_property = triples.mapToPair((
                    PairFunction<Triple, String, Tuple2<String, String>>) trpl ->
                    new Tuple2(trpl.getSubject(), new Tuple2(trpl.getProperty(), trpl.getObject()))
            );

            // 4. Group pairs by subject => s,(p,o)[]
            JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupBySubject = subject_property.groupByKey();

            // 5. Map to pairs (Type,(s,(p,o)[]))
            //@SuppressWarnings({ "serial" })
            JavaPairRDD<String, Tuple2<String, Iterable<Tuple2<String, String>>>> type_s_po = groupBySubject.mapToPair((
                    PairFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, Tuple2<String, Iterable<Tuple2<String, String>>>>) list -> {

                List<Tuple2<String, String>> p_o = new ArrayList<>();
                List<String> types = new ArrayList<>();
                String property;
                String object;
                Tuple2<String, String> tt;
                Tuple2<String, String> t2;

                String subject = list._1();
                for (Tuple2<String, String> stringStringTuple2 : list._2()) {
                    tt = stringStringTuple2;
                    property = tt._1();
                    object = tt._2();
                    if (property == null) {
                        p_o.add(new Tuple2<>("type_" + object, "1"));
                        types.add(object);
                    } else {
                        // Form Tuple2(P,O)
                        t2 = new Tuple2<>(property, object);
                        p_o.add(t2);
                    }
                }

                Collections.sort(types); // order types lexicographically then select the last one => similar instances end up in same table

                //String chosen_type = lastType; // The last type is generally the most specific, but this is definitely not a rule.
                String chosen_type = types.get(types.size()-1);

                // We might use a hierarchy of classes from the schema if provided in future
                p_o.remove(new Tuple2<String, Object>("type_" + chosen_type, "1"));

                Tuple2 s_po = new Tuple2(subject, p_o);
                return new Tuple2<String, Tuple2<String, Iterable<Tuple2<String, String>>>>(chosen_type, s_po);
            });

            // 6. Group by type => (type, It(s, It(p, o)))
            JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> groupByType = type_s_po.groupByKey();

            // 7. Get all the types
            //groupByType: <String, Iterable<Tuple2<String, Iterable<Tuple2<String, Object>>>>>
            // THIS CAN BE SUB-OPTIMAL WITH LARGE DATA.
            List<String> keys = groupByType.keys().distinct().collect();

            System.out.println("Types found: " + keys);
            // 8. Iterate through all types
            //int t = 0;
            //for (String key : keys) {
            //t++;
            //if (t < 20) { // To remove later
            //if(key.contains("HistoricTower")){

            // 8.1 Get RDD of the type
            //@SuppressWarnings("unused")
            JavaRDD<Tuple2<String, Iterable<Tuple2<String, String>>>> rddByKey = getRddByKey(groupByType, className);

            // 8.2 Map the type RDD => Return type columns
            //JavaRDD<LinkedHashMap<String, Object>> cols = rddByKey.map(i -> {
            JavaRDD<String> cols = rddByKey.flatMap((FlatMapFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String>) i -> {
                LinkedHashMap<String, Object> po = new LinkedHashMap<>(); // a hashamp (that keeps order) to store all type's columns

                // 8.2.1 Iterate through all (p,o) and collect the columns (update incrementally the hashmap)

                for (Tuple2<String, String> temp : i._2) {
                    String property = temp._1();
                    String object = reverse(temp._2());

                    if (object.contains("XMLSchema#double")) {
                        if (!po.containsKey(property + "--TD") && !po.containsKey(property + "--TAD"))
                            property = property + "--TD";
                        else if (!po.containsKey(property + "--TAD")) {
                            po.remove(property + "--TD");
                            property = property + "--TAD";
                        }

                    } else if (object.contains("XMLSchema#int")) {
                        property = property + "--TI";

                        if (po.containsKey(property))
                            property = property.replace("--TI", "--TAI");

                    } else if (object.contains("XMLSchema#boolean")) {
                        property = property + "--TB";
                    } else if (object.contains("XMLSchema#dateTime")) {
                        property = property + "--TTS";
                    }

                    if (po.containsKey(property) && !po.containsKey(property + "**")) {
                        po.remove(property);
                        property = property + "**";
                        //System.out.println("Property: " + property);
                    } else if (po.containsKey(property + "**")) {
                        property = property + "**";
                    }

                    po.put(property, ""); // CAUTION: overwriting previous columns
                }

                // 8.2.2 At last, add the id column
                po.put("id", "");

                return po.keySet().iterator();
                //return (Iterator<String>) po.keySet();
            });

            // 8.- Vars
            LinkedHashMap<String, Object> type_columns = new LinkedHashMap<String, Object>(); // a hashamp (that keeps order) to store all type's columns
            String col;

            // 8.3 Read columns and construct a hashmap
            final List<String> readColumns = cols.distinct().collect();

            for (String j : readColumns) type_columns.put(j,""); // Overwrite original columns (collect() may return columns in different order than collected firstly)

            // 8.4 Generate the Parquet table schema from the collected columns
            List<StructField> table_columns = new ArrayList<>();
            HashMap<String,String> toSaveToDB = new HashMap<>();


            for (String s : readColumns) {
                if(s.contains("--TD")) {
                    if(!readColumns.contains(s.split("--")[0] + "--TAD")) {
                        col = s.split("--")[0];
                        table_columns.add(DataTypes.createStructField(col, DataTypes.DoubleType, true));
                        //toSaveToDB.put(col, "double");
                    }
                } else if(s.contains("--TI")) {
                    col = s.split("--")[0];
                    table_columns.add(DataTypes.createStructField(col, DataTypes.IntegerType, true));
                    //toSaveToDB.put(col, "int");
                } else if(s.contains("--TB")) {
                    col = s.split("--")[0];
                    table_columns.add(DataTypes.createStructField(col, DataTypes.BooleanType, true));
                    //toSaveToDB.put(col, "boolean");
                } else if(s.contains("--TTS")) {
                    col = s.split("--")[0];
                    table_columns.add(DataTypes.createStructField(col, DataTypes.TimestampType, true));
                    //toSaveToDB.put(col, "timeDate");
                } else if(s.contains("--TAD")) {
                    col = s.split("--")[0];
                    table_columns.add(DataTypes.createStructField(col, DataTypes.createArrayType(DataTypes.DoubleType, true), true));
                    //toSaveToDB.put(col, "arrayDouble");
                } else if(s.contains("--TAI")) {
                    col = s.split("--")[0];
                    table_columns.add(DataTypes.createStructField(col, DataTypes.createArrayType(DataTypes.IntegerType, true), true));
                    //toSaveToDB.put(col, "arrayInt");
                } else if(s.contains("**")) {
                    col = s.replace("**", "");
                    table_columns.add(DataTypes.createStructField(col, DataTypes.createArrayType(DataTypes.StringType, true), true));
                } else {
                    table_columns.add(DataTypes.createStructField(s, DataTypes.StringType, true));
                    //toSaveToDB.put(s, "string");
                }
            }

            // 8.5 Save columns to database
            //saveToMongoDB(replaceInType(key), toSaveToDB, dsName, dsIRI);

            StructType schema = DataTypes.createStructType(table_columns);

            // 8.6. Map RDD of (subject, Iter(property, object)) to an RDD of Row
            JavaRDD<Row> returnValues = rddByKey.map((Function<Tuple2<String, Iterable<Tuple2<String, String>>>, Row>) i -> {

                Row values_list;
                LinkedHashMap<String, Object> po = new LinkedHashMap<>();

                // 8.6.1 Initialize the hashmap values with null (they're previously initialized with a String "", so if a certain value is an int => a cast error)
                for (String j : readColumns) { // TO INHENCE
                    if(j.contains("--TI"))
                        po.put(j.replace("--TI", ""),null);
                    else if(j.contains("--TD") && !readColumns.contains(j + "--TAD"))
                        po.put(j.replace("--TD", ""),null);
                    else if(j.contains("--TB"))
                        po.put(j.replace("--TB", ""),null);
                    else if(j.contains("--TTS"))
                        po.put(j.replace("--TTS", ""),null);
                    else if(j.contains("--TAI"))
                        po.put(j.replace("--TAI", ""),null);
                    else if(j.contains("--TAD"))
                        po.put(j.replace("--TAD", ""),null);
                    else if(j.contains("**"))
                        po.put(j.replace("**", ""),null);
                    else
                        po.put(j,null);
                }

                // 8.6.2 Iterate through all the (property, object) pairs to save data in the collected columns
                String subject = i._1;

                for(Tuple2<String, String> temp : i._2) {
                    String property = temp._1();
                    String object = reverse(temp._2());
                    Object newobject = null;

                    if (readColumns.contains(property + "--TD") && !readColumns.contains(property + "--TAD")) {
                        newobject = Double.parseDouble(object.replace("^^www.w3.org/2001/XMLSchema#double", "").replace("\"", ""));
                        po.put(property, newobject);
                    } else if (readColumns.contains(property + "--TI")) {
                        newobject = Integer.parseInt(object.replace("^^www.w3.org/2001/XMLSchema#integer", "").replace("^^www.w3.org/2001/XMLSchema#int", "").replace("\"", ""));
                        po.put(property, newobject);
                    } else if (readColumns.contains(property + "--TB")) {
                        newobject = Boolean.parseBoolean(object.replace("^^www.w3.org/2001/XMLSchema#boolean", "").replace("\"", ""));
                        po.put(property, newobject);
                    } else if (readColumns.contains(property + "--TTS")) {
                        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        newobject = Timestamp.valueOf(object.replace("^^www.w3.org/2001/XMLSchema#dateTime", "").replace("\"", "").replace("T", " "));
                        po.put(property, newobject);
                    } else if (readColumns.contains(property + "--TAD")) {
                        ArrayList<Double> arr;
                        newobject = Double.parseDouble(object.replace("^^www.w3.org/2001/XMLSchema#double", "").replace("\"", ""));
                        if (po.get(property) != null) {
                            //System.out.println("TYPE (" + po.get(property) + "): ");
                            arr = (ArrayList<Double>) po.get(property);
                            arr.add((Double) newobject);
                        } else {
                            //System.out.println("TYPE (" + po.get(property) + ")");
                            arr = new ArrayList<>();
                            arr.add((Double) newobject);
                        }
                        po.put(property, arr);
                    } else if (readColumns.contains(property + "--TAI")) {
                        ArrayList<Integer> arr = new ArrayList<>();
                        if (po.containsKey(property)) {
                            arr = (ArrayList<Integer>) po.get(property);
                            arr.add((Integer) newobject);
                        } else {
                            arr.add((Integer) newobject);
                        }
                        po.put(property, arr);
                    } else if (readColumns.contains(property + "**")) {
                        //ArrayList<String> arr = new ArrayList<String>();
                        ArrayList<String> temparr; // In new Parquet, ArrayString type saves only String[]s not ArrayLists, so needs to change back and forth from String to ArrayList String
                        String[] arr;
                        newobject = object.replace("**", "").replace("\"", "");
                        if (po.get(property) != null) {
                            //System.out.println("TYPE (" + po.get(property) + "): ");

                            arr = (String[]) po.get(property);
                            temparr = new ArrayList<>(Arrays.asList(arr));
                            //arr = (ArrayList<String>) po.get(property);
                            // create arraylist
                            temparr.add((String) newobject);

                            arr = temparr.toArray(new String[0]);
                        } else {
                            arr = new String[]{(String) newobject};
                            //arr = new ArrayList<String>();
                            //arr.add((String) newobject);
                        }
                        //String[] ary = new String[arr.size()];
                        //ary = arr.toArray(ary);
                        po.put(property, arr);
                    } else
                        po.put(property, object);
                }

                // 8.6.3 Add the subject finally as the ID to the hashmap
                po.put("id", subject);

                //System.out.println("Values to be inserted under this schema: " + po.keySet());

                // 8.6.4 Create the row from the hashmap values
                List<Object> vals = new ArrayList<>(po.values());
                values_list = RowFactory.create(vals.toArray());

                return values_list;
            });

            /*returnValues.collect().forEach(row -> {
                System.out.println(row.toString());
            });*/

            //System.out.println("returnValues: " + returnValues);
            //System.out.println("schema: " + schema);

            // 8.7 Create an RDD by applying a schema to the RDD
            /*Dataset typeDataFrame = spark.createDataFrame(returnValues, schema);*/
            Dataset typeDataFrame = spark.createDataFrame(returnValues, schema);

            // 8.8 Save to Parquet table
            //typeDataFrame.write().parquet(output_path + replaceInType(key));
            //}
            //}
            //ctx.close();
            //spark.stop();

            return typeDataFrame;
        } catch (Exception ex) {
            System.out.println("SOMETHING WENT WRONG..." + ex.getMessage());
            //spark.stop();

            spark.close();
        }

        return null;
    }

    private String reverse(String string) {
        return new StringBuffer(string).reverse().toString();
    }

    private JavaRDD getRddByKey(JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> pairRdd, String key) {

        JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> a = pairRdd.filter((
                Function<Tuple2<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>>, Boolean>) v -> {
            // TODO Auto-generated method stub
            return v._1().equals(key);
        });

        /*return a.values().flatMap(tuples -> tuples.iterator());*/
        return a.values().flatMap(tuples -> tuples.iterator());
    }

    // Helping methods
    private String removeTagSymbol(String string) {
        return string.replace("<", "").replace(">", "");
    }

    private String replaceInValue(String str) {
        return str.replace("http://", "");
    }

    private String replaceInType(String str) {
        return str.replace("/", "__").replace("-", "@");
    }

    private String replaceInColumn(String str) {
        return str.replace("http://", "");
    }

}
