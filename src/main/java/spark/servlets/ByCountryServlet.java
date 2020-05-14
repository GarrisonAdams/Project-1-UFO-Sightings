package spark.servlets;

import spark.RDDCustomOperations;
import spark.CustomLists.CustomListString;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class ByCountryServlet extends HttpServlet {

    JavaSparkContext sparkContext;
    JavaRDD<String> inputFile;

    private static final long serialVersionUID = 1L;

    public ByCountryServlet(JavaSparkContext context, JavaRDD<String> rdd)
    {
        this.sparkContext = context;
        this.inputFile = rdd;
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
    {
        resp.getWriter().println("Here is the sightingsByCountry page");
        resp.getWriter().println("List of Commands: ");
        resp.getWriter().println("?inputType=inCountry&country=(yourcountryhere)    returns number of sightings in a country");
        resp.getWriter().println("?inputType=numberOfCases&country=(yourcountryhere)     returns the number of sightings in a country");
        resp.getWriter().println("?inputType=byCountry         returns the number of sightings in each country");
        resp.getWriter().println();
        resp.getWriter().println("List of mappings: time,state,project");
        resp.getWriter().println();
        resp.getWriter().println();

        if(req.getParameter("inputType").equals("inCountry"))
        {
            resp.getWriter().println("Number of Cases in " + req.getParameter("country"));
            resp.getWriter().println(numberOfCasesInCountry(req.getParameter("country")));
            resp.getWriter().println();
            resp.getWriter().println("Number of sightings in " + req.getParameter("country"));
            resp.getWriter().println(new CustomListString(sightingsInCountry(req.getParameter("country")).sortByKey().collect()));
        }
        else if(req.getParameter("inputType").equals("byCountry"))
        {
            resp.getWriter().println("Sightings by country:");
            resp.getWriter().println(new CustomListString(sightingsByCountry().sortByKey().collect()));
        }
        else
        {
            resp.getWriter().println("Invalid input.");
        }
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