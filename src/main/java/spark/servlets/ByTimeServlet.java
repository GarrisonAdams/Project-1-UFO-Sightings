package spark.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import spark.RDDCustomOperations;
import spark.CustomLists.CustomListInteger;
import spark.CustomLists.CustomListString;

public class ByTimeServlet extends HttpServlet {

    JavaSparkContext sparkContext;
    JavaRDD<String> inputFile;

    private static final long serialVersionUID = 1L;

    public ByTimeServlet(JavaSparkContext context, JavaRDD<String> rdd)
    {
        this.sparkContext = context;
        this.inputFile = rdd;
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
    {
        resp.getWriter().println("Here is the SightingsByTime page");
        resp.getWriter().println();
        resp.getWriter().println("List of mappings: time,state,country");
        resp.getWriter().println();
        resp.getWriter().println();


            resp.getWriter().println("List of sightings by hour: ");
            resp.getWriter().println(new CustomListString(sightingsByHour().sortByKey().collect()));
            resp.getWriter().println();

            resp.getWriter().println("List of sightings by month: ");
            resp.getWriter().println(new CustomListInteger(sightingsByMonth().sortByKey().collect()));
            resp.getWriter().println();

            resp.getWriter().println("List of sightings by year: ");
            resp.getWriter().println(new CustomListString(sightingsByYear().sortByKey().collect()));
            resp.getWriter().println();

            int i = 1943;
            while(i < 2015)
            {
                resp.getWriter().println("Percentage increase/decrease between year " + i +" and " + (i+10)); 
                resp.getWriter().println(computePercentageChange(i,i+10));
                i += 10;
            }

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

    public double computePercentageChange(int year1, int year2)
    { 
        //Returns the rows that have year1 or year2
        JavaRDD<String> rdd2 = inputFile.filter(x -> {
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

        JavaPairRDD<String, Integer> rdd4 = RDDCustomOperations.rddCounterString(rdd3);

        double firstKey = rdd4.lookup(String.valueOf(year1)).get(0);
        double secondKey = rdd4.lookup(String.valueOf(year2)).get(0);
               
        return (secondKey -  firstKey) /  firstKey * 100;
    }
}