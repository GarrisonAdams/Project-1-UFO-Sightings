package spark.servlets;
import spark.RDDCustomOperations;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class ProjectServlet extends HttpServlet {

    JavaSparkContext sparkContext;
    JavaRDD<String> inputFile;

    private static final long serialVersionUID = 1L;

    public void init() throws ServletException {

    }

    public ProjectServlet(JavaSparkContext context, JavaRDD<String> rdd)
    {
        this.sparkContext = context;
        this.inputFile = rdd;
    }


    public int numberOfCasesInYear(JavaRDD<String> rdd, int year)
    {
        JavaRDD<String> rdd2 = rdd.filter(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[0].split(" ");
                String[] splitRow3 = splitRow2[0].split("/");

                if(splitRow3[2].equals("") || splitRow3[2].equals(null))
                    return false;

                return Integer.parseInt(splitRow3[2]) == year;
            } catch (Exception e) {
                System.out.println("Exception inside rdd2 of numberOfCasesInYear");
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
                System.out.println("Exception inside rdd3 of numberOfCasesInYear");
                e.printStackTrace();
            }
            return "";
        });

        JavaPairRDD<String, Integer> rdd4 = RDDCustomOperations.rddCounterString(rdd3);

        return rdd4.lookup(String.valueOf(year)).get(0);         
    
    }




    public JavaPairRDD<String,Integer> sightingsByShape(JavaRDD<String> rdd)
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddStripToColumn(rdd,4);
        JavaPairRDD<String,Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);

        return rdd3;
    }
    
    public JavaPairRDD<String,Integer> listOfAllCases (JavaRDD<String> rdd)
    {
        JavaRDD<String> rdd2 = rdd.map(x -> {
            String[] splitRow = x.split(",");
  
            return splitRow[2] + "," + splitRow[3];
        });

        JavaPairRDD<String,Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);
 
        return rdd3;
    }
    
    public JavaPairRDD<String,Integer> listByDatesPosted(JavaRDD<String> rdd)
    {
        JavaRDD<String> rdd2 = rdd.map(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[8].split("/");
                return splitRow2[2];
            } catch (Exception e) {
                System.out.println("Exception inside rdd2 of listOfDatesPosted");
                e.printStackTrace();
            }
            return "";
        });

        return RDDCustomOperations.rddCounterString(rdd2);
    }





    
   
    public JavaPairRDD<String,Integer> bySightingDuration()
    {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddStripToColumn(inputFile,5);

        JavaPairRDD<String,Integer> rdd3 = RDDCustomOperations.rddCounterString(rdd2);

        return rdd3;
    }







    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
        resp.getWriter().println("In project");
        resp.getWriter().println("BySightingsDuration");
        resp.getWriter().println(bySightingDuration().sortByKey().collect());


        
    }

}