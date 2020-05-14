package spark.servlets;

import spark.database.DatabaseOperations;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



public class ByCountryServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public ByCountryServlet()
    {
        
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
    {
        resp.getWriter().println("Here is the ByCountry page");
        resp.getWriter().println("List of Commands: ");
        resp.getWriter().println("?inputType=inCountry&country=(yourcountryhere)    returns number of sightings in a country");
        resp.getWriter().println("?inputType=byCountry         returns the number of sightings in each country");
        resp.getWriter().println();
        resp.getWriter().println("List of mappings: time,state,project");
        resp.getWriter().println();
        resp.getWriter().println();
        try {
        if(req.getParameter("inputType").equals("inCountry"))
        {
            resp.getWriter().println("Number of sightings in " + req.getParameter("country"));
            resp.getWriter().println(DatabaseOperations.readFromDatabase("inCountryTable", req.getParameter("country")));
            resp.getWriter().println();
            resp.getWriter().println(DatabaseOperations.printDatabase("inCountryTable", "String"));
        }
        else if(req.getParameter("inputType").equals("byCountry"))
        {
            resp.getWriter().println("Sightings by country:");
            resp.getWriter().println(DatabaseOperations.printDatabase("byCountryTable", "String"));
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
}