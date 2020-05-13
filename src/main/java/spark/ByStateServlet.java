package spark;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

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
        resp.getWriter().println("Here is the sightingsByState page");
        resp.getWriter().println("List of Commands: ");
        resp.getWriter().println("?inputType=inState&state=(yourstatehere)    returns number of sightings in a state");
        resp.getWriter().println("?inputType=numberOfCases&state=(yourstatehere)     returns the number of sightings in a state");
        resp.getWriter().println("?inputType=byState         returns the number of sightings in each state");
        resp.getWriter().println();
        resp.getWriter().println();
        resp.getWriter().println();

        if(req.getParameter("inputType").equals("inState"))
        {
            resp.getWriter().println("Number of sightings in " + req.getParameter("state"));
            resp.getWriter().println(sightingsInState(req.getParameter("state")).sortByKey().collect());
        }
        else if(req.getParameter("inputType").equals("numberOfCases"))
        {
            resp.getWriter().println("Number of Cases in " + req.getParameter("state"));
            resp.getWriter().println(numberOfCasesInState(req.getParameter("state")));
        }
        else if(req.getParameter("inputType").equals("byState"))
        {
            resp.getWriter().println("Sightings by state:");
            resp.getWriter().println(sightingsByState().sortByKey().collect());
        }
        else
        {
            resp.getWriter().println("Invalid input.");
        }
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

}