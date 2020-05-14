package spark.servlets;

import spark.database.DatabaseOperations;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ByStateServlet extends HttpServlet {

    public ByStateServlet()
    {

    }

    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException 
    {
        resp.getWriter().println("Here is the ByState page");
        resp.getWriter().println("List of Commands: ");
        resp.getWriter().println("?inputType=inState&state=(yourstate)    returns number of sightings in a state");
        resp.getWriter().println("?inputType=bystate         returns the number of sightings in each state");
        resp.getWriter().println();
        resp.getWriter().println("List of mappings: time,state,project");
        resp.getWriter().println();
        resp.getWriter().println();
        try {
        if(req.getParameter("inputType").equals("inState"))
        {
            resp.getWriter().println("Number of sightings in " + req.getParameter("state"));
            resp.getWriter().println(DatabaseOperations.readFromDatabase("inStateTable", req.getParameter("state")));
            resp.getWriter().println();
            resp.getWriter().println(DatabaseOperations.printDatabase("inStateTable", "String"));
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

}