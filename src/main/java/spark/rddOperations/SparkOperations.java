package spark.rddoperations;

import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import spark.database.DatabaseOperations;

public class SparkOperations {
    
    JavaSparkContext sparkContext;
    JavaRDD<String> inputFile;

    public SparkOperations(JavaSparkContext context, JavaRDD<String> input) {
        this.sparkContext = context;
        this.inputFile = input;
    }

    public void runOperations() throws SQLException {
        DatabaseOperations.insertIntoDatabase(sightingsByHour().sortByKey().collect(), "byHourTable");
        DatabaseOperations.insertIntoDatabase(sightingsByYear().sortByKey().collect(), "byYearTable");
        DatabaseOperations.insertIntoDatabase(sightingsByMonth().sortByKey().collect(), "byMonthTable", true);
        DatabaseOperations.insertIntoDatabase(sightingsByCountry().sortByKey().collect(), "byCountryTable");
        DatabaseOperations.insertIntoDatabase(sightingsByState().sortByKey().collect(), "byStateTable");
        DatabaseOperations.insertIntoDatabase(sightingsByShape().sortByKey().collect(), "byShapeTable");

        DatabaseOperations.insertIntoDatabase(sightingsByDuration().sortByKey().collect(), "byDurationTable");

    }

    public JavaPairRDD<String, Integer> sightingsInState(String state) {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile, state.toLowerCase(), 2);

        JavaRDD<String> rdd3 = rdd2.map(x -> {
            String[] splitRow = x.split(",");
            return splitRow[1] + "," + splitRow[2];
        });

        return RDDCustomOperations.rddCounterString(rdd3);
    }

    public int numberOfCasesInState(String state) {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile, state.toLowerCase(), 2);
        JavaRDD<String> rdd3 = RDDCustomOperations.rddStripToColumn(rdd2, 2);
        JavaPairRDD<String, Integer> rdd4 = RDDCustomOperations.rddCounterString(rdd3);

        return rdd4.lookup(state.toLowerCase()).get(0);
    }

    public JavaPairRDD<String, Integer> sightingsByState() {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddStripToColumn(inputFile, 2);
        JavaPairRDD<String, Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);

        return rdd3;
    }

    public JavaPairRDD<String, Integer> sightingsByYear() {
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

    public JavaPairRDD<String, Integer> sightingsByHour() {
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
            return "";
        });

        JavaPairRDD<String, Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
        return rdd3;
    }

    public JavaPairRDD<Integer, Integer> sightingsByMonth() {
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

    public JavaPairRDD<String, Integer> sightingsByCountry() {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddStripToColumn(inputFile, 3);
        JavaPairRDD<String, Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);

        return rdd3;
    }

    public JavaPairRDD<String, Integer> sightingsByDuration() {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddStripToColumn(inputFile, 5);
        // JavaRDD<Double> rdd3 = rdd2.map(x ->
        // {
        //     return Double.parseDouble(x) / 25 *;
        // });

        JavaPairRDD<String, Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
        
        return rdd3;
    }

    public JavaPairRDD<String, Integer> sightingsInCountry(String country) {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile, country.toLowerCase(), 3);

        JavaRDD<String> rdd3 = rdd2.map(x ->
        {
            String[] splitRow = x.split(",");
            return splitRow[2]+","+splitRow[3];
        });

        return RDDCustomOperations.rddCounterString(rdd3);
    }

    public JavaPairRDD<String,Integer> sightingsByShape()
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddStripToColumn(inputFile, 4);
        JavaPairRDD<String, Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
        return rdd3;
    }

    public int numberOfCasesInCountry(String country)
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile,country.toLowerCase(),3);
        JavaRDD<String> rdd3 = RDDCustomOperations.rddStripToColumn(rdd2,3);
        JavaPairRDD<String,Integer> rdd4 = RDDCustomOperations.rddCounterString(rdd3);

        return rdd4.lookup(country.toLowerCase()).get(0);
    }



    

}
