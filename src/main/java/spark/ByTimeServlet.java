package spark;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

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
        resp.getWriter().println("List of Commands: ");
        resp.getWriter().println("?timeperiod=byHour   lists the cases by the hour in which it ocurred");
        resp.getWriter().println("?timeperiod=byMonth   lists the cases by the month in which it ocurred");
        resp.getWriter().println("?timeperiod=byYear   lists the cases by the year in which it ocurred");
        resp.getWriter().println();


        if(req.getParameter("timeperiod").equals("byHour"))
        {
            resp.getWriter().println("List of sightings by hour: ");
            resp.getWriter().println(sightingsByHour().sortByKey().collect());
        }
        else if(req.getParameter("timeperiod").equals("byMonth"))
        {
            resp.getWriter().println("List of sightings by month: ");
            resp.getWriter().println(sightingsByMonth().sortByKey().collect());
        }
        else if(req.getParameter("timeperiod").equals("byYear"))
        {
            resp.getWriter().println("List of sightings by year: ");
            resp.getWriter().println(sightingsByYear().sortByKey().collect());
        }
        else
        {
            resp.getWriter().println("Invalid input.");
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

    public JavaPairRDD<String,Integer> sightingsByMonth()
    {
        JavaRDD<String> rdd2 = inputFile.map(x -> {
            try {
                String[] splitRow = x.split(",");
                String[] splitRow2 = splitRow[0].split(" ");
                String[] splitRow3 = splitRow2[0].split("/");
                return splitRow3[0];
            } catch (Exception e) {
                System.out.println("Exception inside rdd2 of listOfCasesByYear");
                e.printStackTrace();
            }
            return "";
        });

        JavaRDD<String> rdd3 = rdd2.filter(x -> {

            return !x.equals("datetime");
        });


        JavaPairRDD<String, Integer> rdd4 = RDDCustomOperations.rddCounterString(rdd3);

        return rdd4;
    }
}