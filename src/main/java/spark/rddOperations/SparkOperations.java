package spark.rddoperations;

import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import spark.database.DatabaseOperations;

public class SparkOperations {
    

    SparkConf conf = new SparkConf().setAppName("Project1").setMaster("local[8]");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
    JavaRDD<String> inputFile = sparkContext.textFile("C:\\Users\\Garrison\\Project-1-Garrison\\src\\main\\resources\\scrubbed.csv", 4);
   
 

    public void runOperations() throws SQLException {
        DatabaseOperations.insertIntoDatabase(sightingsByHour().sortByKey().collect(), "byHourTable");
        DatabaseOperations.insertIntoDatabase(sightingsByYear().sortByKey().collect(), "byYearTable");
        DatabaseOperations.insertIntoDatabase(sightingsByMonth().sortByKey().collect(), "byMonthTable",true);
        DatabaseOperations.insertIntoDatabase(sightingsByCountry().sortByKey().collect(), "byCountryTable");
        DatabaseOperations.insertIntoDatabase(sightingsInCountry("us").sortByKey().collect(), "InCountryTable");
        DatabaseOperations.insertIntoDatabase(sightingsByState().sortByKey().collect(), "byStateTable");
        DatabaseOperations.insertIntoDatabase(sightingsInState("ca").sortByKey().collect(), "InStateTable");

        
        
        // System.out.println(DatabaseOperations.printDatabase("byHourTable", "String"));
        // System.out.println(DatabaseOperations.printDatabase("byYearTable", "String"));
        // System.out.println(DatabaseOperations.printDatabase("byMonthTable", "integer"));
        // System.out.println(DatabaseOperations.printDatabase("byCountryTable", "String"));
        // System.out.println(DatabaseOperations.printDatabase("inCountryTable", "String"));
        // System.out.println(DatabaseOperations.printDatabase("byStateTable", "String"));
        // System.out.println(DatabaseOperations.printDatabase("inStateTable", "String"));

        sparkContext.close();
    }

    public JavaPairRDD<String,Integer> sightingsInState(String state)
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile,state.toLowerCase(),2);

        JavaRDD<String> rdd3 = rdd2.map(x ->
        {
            String[] splitRow = x.split(",");
            return splitRow[1] + "," + splitRow[2];
        });

        return RDDCustomOperations.rddCounterString(rdd3);
    }

    public int numberOfCasesInState(String state)
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile,state.toLowerCase(),2);
        JavaRDD<String> rdd3 = RDDCustomOperations.rddStripToColumn(rdd2,2);
        JavaPairRDD<String,Integer> rdd4 = RDDCustomOperations.rddCounterString(rdd3);
 
        return rdd4.lookup(state.toLowerCase()).get(0);
    }

    public JavaPairRDD<String,Integer> sightingsByState()
    {
        JavaRDD<String> rdd2 =  RDDCustomOperations.rddStripToColumn(inputFile, 2);
        JavaPairRDD<String,Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
 
        return rdd3;
    }
        
    public JavaPairRDD<String,Integer> sightingsByYear()
    {
        JavaRDD<String> rdd2 = inputFile.map(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[0].split(" ");
                String[] splitRow3 = splitRow2[0].split("/");
                return splitRow3[2];
            } catch (Exception e) {
                System.out.println("Exception inside rdd2 of listOfCasesByYear");
                e.printStackTrace();
            }
            return "";
        });

        JavaPairRDD<String, Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
        return rdd3;
  
    }
    
    public JavaPairRDD<String,Integer> sightingsByHour()
    {
        JavaRDD<String> rdd2 = inputFile.map(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[0].split(" ");
                String[] splitRow3 = splitRow2[1].split(":");
                return splitRow3[0];
            } catch (Exception e) {
                System.out.println("Exception inside rdd2 of listOfCasesByYear");
                e.printStackTrace();
            }
            return "";});

            JavaPairRDD<String, Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
            return rdd3;
    }

    public JavaPairRDD<Integer,Integer> sightingsByMonth()
    {
        JavaRDD<Integer> rdd2 = inputFile.map(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[0].split(" ");
                String[] splitRow3 = splitRow2[0].split("/");
                return Integer.parseInt(splitRow3[0]);
            } catch (Exception e) {
                System.out.println("Exception inside rdd2 of listOfCasesByYear");
                e.printStackTrace();
            }
            return 0;
        });

        JavaPairRDD<Integer, Integer> rdd3 = RDDCustomOperations.rddCounterInteger(rdd2);

        return rdd3;
    }


    public JavaPairRDD<String,Integer> sightingsByCountry()
    {
        JavaRDD<String> rdd2 =  RDDCustomOperations.rddStripToColumn(inputFile, 3);
        JavaPairRDD<String,Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
 
        return rdd3;
    }

    public JavaPairRDD<String,Integer> sightingsInCountry(String country)
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile,country.toLowerCase(),3);

        JavaRDD<String> rdd3 = rdd2.map(x ->
        {
            String[] splitRow = x.split(",");
            return splitRow[2]+","+splitRow[3];
        });

        return RDDCustomOperations.rddCounterString(rdd3);
    }

    public int numberOfCasesInCountry(String country)
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile,country.toLowerCase(),3);
        JavaRDD<String> rdd3 = RDDCustomOperations.rddStripToColumn(rdd2,3);
        JavaPairRDD<String,Integer> rdd4 = RDDCustomOperations.rddCounterString(rdd3);

        return rdd4.lookup(country.toLowerCase()).get(0);
    }


    

}
