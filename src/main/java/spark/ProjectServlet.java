package spark;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

//import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;


import scala.Tuple2;

public class ProjectServlet extends HttpServlet {

    JavaSparkContext sparkContext;

    private static final long serialVersionUID = 1L;

    public void init() throws ServletException {

    }

    public ProjectServlet(JavaSparkContext context)
    {
        this.sparkContext = context;
    }

    public JavaRDD<String> rddFiltering(JavaRDD<String> rdd, String text, int column) {
        
        JavaRDD<String> rdd2 = rdd.filter(x -> {
            String[] splitRow = x.split(",");
  
            return splitRow[column].equals(text);
        });

        return rdd2;
    }

    public JavaRDD<String> rddreduceToKey(JavaRDD<String> rdd, int column)
    {
        JavaRDD<String> rdd2 = rdd.map(x ->
        {
            String[] splitRow = x.split(",");
            return splitRow[column];
        });

        return rdd2;


    }
   
    public JavaPairRDD<String, Integer> rddCounterString(JavaRDD<String> rdd) {
        JavaPairRDD<String, Integer> rdd2 = rdd.mapToPair((x) -> new Tuple2<>(x, 1));
        return rdd2.reduceByKey((x, y) -> (x + y));

    }

    public double computePercentageChange(JavaRDD<String> rdd,int year1, int year2)
    { 
        //Returns the rows that have year1 or year2
        JavaRDD<String> rdd2 = rdd.filter(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[0].split(" ");
                String[] splitRow3 = splitRow2[0].split("/");

                if(splitRow3[2].equals("") || splitRow3[2].equals(null))
                    return false;

                return Integer.parseInt(splitRow3[2]) == year1 || Integer.parseInt(splitRow3[2]) == year2;
            } catch (Exception e) {
                System.out.println("Exception inside rdd2 of computePercentageChange");
                e.printStackTrace();
            }
            return false;
        });

        JavaRDD<String> rdd3 = rdd2.map(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[0].split(" ");
                String[] splitRow3 = splitRow2[0].split("/");
                return splitRow3[2];
            } catch (Exception e) {
                System.out.println("Exception inside rdd3 of computePercentageChange");
                e.printStackTrace();
            }
            return "";
        });

        JavaPairRDD<String, Integer> rdd4 = rddCounterString(rdd3);

        double firstKey = rdd4.lookup(String.valueOf(year1)).get(0);
        double secondKey = rdd4.lookup(String.valueOf(year2)).get(0);
               
        return (secondKey -  firstKey) /  firstKey * 100;
    }

    public int numberOfCasesByState(JavaRDD<String> rdd,String state)
    {
        JavaRDD<String> rdd2 = rddFiltering(rdd,state.toLowerCase(),2);
        JavaRDD<String> rdd3 = rddreduceToKey(rdd2,2);
        JavaPairRDD<String,Integer> rdd4 = rddCounterString(rdd3);
 
        return rdd4.lookup(state.toLowerCase()).get(0);
    }
    
    public int numberOfCasesByCountry(JavaRDD<String> rdd,String country)
    {
        JavaRDD<String> rdd2 = rddFiltering(rdd,country.toLowerCase(),3);
        JavaRDD<String> rdd3 = rddreduceToKey(rdd2,3);
        JavaPairRDD<String,Integer> rdd4 = rddCounterString(rdd3);

        return rdd4.lookup(country.toLowerCase()).get(0);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
        JavaRDD<String> rdd = sparkContext
                 .textFile("C:\\Users\\Garrison\\Project-1-Garrison\\src\\main\\resources\\scrubbed.csv", 4);

        double change = computePercentageChange(rdd,2000,2005);
        resp.getWriter().println("Percentage Change between 2000 and 2005");
        resp.getWriter().println(change);


        resp.getWriter().println("Number of sightings in the state of CA");
        int sightingsInCA = numberOfCasesByState(rdd,"ca");
        resp.getWriter().println(sightingsInCA);

        resp.getWriter().println("Number of sightings in the US");
        int sightingsInUS = numberOfCasesByCountry(rdd,"us");
        resp.getWriter().println(sightingsInUS);
    
    
    }

}