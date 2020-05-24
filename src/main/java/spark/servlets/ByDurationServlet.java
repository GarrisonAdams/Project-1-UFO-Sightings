package spark.servlets;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import spark.database.DatabaseOperations;

public class ByDurationServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public ByDurationServlet() {

    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().println("This is the ByDurationServlet");
        resp.getWriter().println("All times are in seconds");
        resp.getWriter().println();
        resp.getWriter().println("Valid mappings: time,country,state,shape,duration");
        try {
            resp.getWriter().println(DatabaseOperations.printDatabase("byDurationTable","string"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}