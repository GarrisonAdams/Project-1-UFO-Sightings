package spark.servlets;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import spark.database.DatabaseOperations;
/**
 * This servlet is mapped to "/spark/time".
 * ?inputType=byHour displays the sightings by the hour in which they took place
 * ?inputType=byMonth displays the sightings by the month in which they took place
 * ?inputType=byYear displays the sightings by the year in which they took place
 */
public class ByTimeServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public ByTimeServlet() {

    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().println("This is the byTimeServlet");
        resp.getWriter().println("List of Commands:");
        resp.getWriter().println("?inputType=byHour");
        resp.getWriter().println("?inputType=byMonth");
        resp.getWriter().println("?inputType=byYear");
        resp.getWriter().println("Valid mappings: time,country,state,shape,duration");
        resp.getWriter().println();
        resp.getWriter().println();

        try {
            if (req.getParameter("inputType").equals("byHour")) {
                resp.getWriter().println("List of sightings by hour: ");
                resp.getWriter().println(DatabaseOperations.printDatabase("byHourTable","string"));
                resp.getWriter().println();
                resp.getWriter().println();
                resp.getWriter().println();
                resp.getWriter().println("Difference between smallest number of sightings and the largest: ");
                resp.getWriter().println(computePercentageChange("08", "21", "byHourTable"));
            } else if (req.getParameter("inputType").equals("byMonth")) {
                resp.getWriter().println("List of sightings by month: ");
                resp.getWriter().println(DatabaseOperations.printDatabase("byMonthTable","integer"));
                resp.getWriter().println();
                resp.getWriter().println();
                resp.getWriter().println();
                resp.getWriter().println();
                resp.getWriter().println("Difference between smallest number of sightings and the largest: ");
                resp.getWriter().println(computePercentageChange("2", "7", "byMonthTable"));
            } else if ((req.getParameter("inputType").equals("byYear"))) {
                resp.getWriter().println("List of sightings by year: ");
                resp.getWriter().println(DatabaseOperations.printDatabase("byYearTable","string"));
                resp.getWriter().println();

                int i = 1943;
                while(i <2010)
                {
                    resp.getWriter().println("percentage increase/decrease between " + i + " and " + (i+10));
                    resp.getWriter().println(computePercentageChange(String.valueOf(i), String.valueOf(i+10), "byYearTable"));
                    i += 10;
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public double computePercentageChange(String time1, String time2, String table) {
        int cases1=0;
        int cases2=0;
        try {
            cases1 = DatabaseOperations.readFromDatabase(table, time1);
            cases2 = DatabaseOperations.readFromDatabase(table, time2);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ((double)cases2 - (double) cases1) /  (double)cases1 * 100;
    }

}