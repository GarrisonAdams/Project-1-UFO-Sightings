package spark.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import spark.database.DatabaseOperations;



public class ByTimeServlet extends HttpServlet {


    private static final long serialVersionUID = 1L;

    public ByTimeServlet()
    {

    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
    {
        resp.getWriter().println("Here is the SightingsByTime page");
        resp.getWriter().println();
        resp.getWriter().println("List of mappings: time,state,country");
        resp.getWriter().println();
        resp.getWriter().println();

        try {
            resp.getWriter().println("List of sightings by hour: ");
            resp.getWriter().println(DatabaseOperations.printDatabase("byHourTable", "String"));
            resp.getWriter().println();

            resp.getWriter().println("List of sightings by month: ");
            resp.getWriter().println(DatabaseOperations.printDatabase("byMonthable", "integer"));
            resp.getWriter().println();

            resp.getWriter().println("List of sightings by year: ");
            resp.getWriter().println(DatabaseOperations.printDatabase("byYearTable", "String"));
            resp.getWriter().println();

            int i = 1943;
            while(i < 2015)
            {
                resp.getWriter().println("Percentage increase/decrease between year " + i +" and " + (i+10)); 
                resp.getWriter().println(computePercentageChange(i,i+10));
                i += 10;
            }
        }  catch(Exception e)
        {
            e.printStackTrace();
        }

    }

    public double computePercentageChange(int year1, int year2)
    { 
        return (year2 -  year1) /  year1 * 100;
    }
    

    
}