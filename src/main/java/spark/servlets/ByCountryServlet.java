package spark.servlets;

import spark.customlists.CustomListString;
import spark.database.DatabaseOperations;
import spark.rddoperations.RDDCustomOperations;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This servlet is mapped to "/spark/country".
 * ?inputType=byCountry displays information about the number of UFO cases by country
 * ?inputType=inCountry&country=(Country) displays information about the UFO cases in a country
    For the inCountry inputType, the RDD operations are performed here and not mapped to database.
 */
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
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().println("This is the ByCountryServlet");
        resp.getWriter().println("List of Commands: ");
        resp.getWriter().println("?inputType=inCountry&country=(yourcountryhere)    returns number of sightings in a country");
        resp.getWriter().println("?inputType=byCountry         returns the number of sightings in each country");
        resp.getWriter().println();
        resp.getWriter().println("Valid mappings: time,country,state,shape,duration");
        resp.getWriter().println();
        resp.getWriter().println();

        if (req.getParameter("inputType").equals("inCountry")) {
            resp.getWriter().println("Number of sightings in " + req.getParameter("country"));
            try {
                resp.getWriter().println(DatabaseOperations.readFromDatabase("byCountryTable", req.getParameter("country")));
                resp.getWriter().println(new CustomListString(sightingsInCountry(req.getParameter("country")).sortByKey().collect()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else if (req.getParameter("inputType").equals("byCountry")) {
            resp.getWriter().println("Sightings by country:");
            try {
                resp.getWriter().println(DatabaseOperations.printDatabase("byCountryTable","string"));
            } catch (SQLException e) {
                e.printStackTrace();
            }
            
        }
        else
        {
            resp.getWriter().println("Invalid input.");
        }
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
}