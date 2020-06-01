package spark.servlets;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import spark.database.DatabaseOperations;
/**
 * This servlet is mapped to "/spark/shape".
 * This servlet displays information about the frequency of the shapes of UFOs
 */
public class ByShapeServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public ByShapeServlet() {

    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.getWriter().println("This is the ByShapeServlet");
        resp.getWriter().println();
        resp.getWriter().println("Valid mappings: time,country,state,shape,duration");
        try {
            resp.getWriter().println(DatabaseOperations.printDatabase("byShapeTable","string"));
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}