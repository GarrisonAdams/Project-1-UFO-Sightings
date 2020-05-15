package spark.servlets;

import spark.customlists.CustomListString;
import spark.database.DatabaseOperations;
import spark.rddoperations.RDDCustomOperations;
import spark.rddoperations.SparkOperations;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ByStateServlet extends HttpServlet {


    JavaSparkContext sparkContext;
    JavaRDD<String> inputFile;

    private static final long serialVersionUID = 1L;

    public ByStateServlet(JavaSparkContext context, JavaRDD<String> rdd)
    {
        this.sparkContext = context;
        this.inputFile = rdd;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
    {
        resp.getWriter().println("This is the ByStateServlet");
        resp.getWriter().println("List of Commands: ");
        resp.getWriter().println("?inputType=inState&state=(yourstate)    returns number of sightings in a state");
        resp.getWriter().println("?inputType=bystate         returns the number of sightings in each state");
        resp.getWriter().println();
        resp.getWriter().println("Valid mappings: time,country,state,shape,duration");
        resp.getWriter().println();
        resp.getWriter().println();
        try {
        if(req.getParameter("inputType").equals("inState"))
        {  
            resp.getWriter().println("Number of sightings in " + req.getParameter("state"));
            resp.getWriter().println(DatabaseOperations.readFromDatabase("byStateTable", req.getParameter("state")));
            resp.getWriter().println(new CustomListString(sightingsInState(req.getParameter("state")).sortByKey().collect()));
        }
        else if(req.getParameter("inputType").equals("byState"))
        {
            resp.getWriter().println("Sightings by state:");
            resp.getWriter().println(DatabaseOperations.printDatabase("byStateTable", "String"));
        }
        else
        {
            resp.getWriter().println("Invalid input.");
        }
    } catch(Exception e)
    {
        e.printStackTrace();
    }
    }

    public JavaPairRDD<String, Integer> sightingsInState(String state) {
        JavaRDD<String> rdd2 = RDDCustomOperations.rddFiltering(inputFile, state.toLowerCase(), 2);

        JavaRDD<String> rdd3 = rdd2.map(x -> {
            String[] splitRow = x.split(",");
            return splitRow[1] + "," + splitRow[2];
        });

        return RDDCustomOperations.rddCounterString(rdd3);
    }
    

}