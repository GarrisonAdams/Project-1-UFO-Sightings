package spark;

import java.io.File;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Wrapper;
import org.apache.catalina.startup.Tomcat;

public class Server {
    public static void main(String[] args) throws LifecycleException {
        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
        tomcat.setPort(8080);
        tomcat.getConnector();
        tomcat.addWebapp("/spark", new File("src/main/").getAbsolutePath());
        Wrapper helloServlet = tomcat.addServlet("/spark", "Servlet", new Servlet());
        helloServlet.addMapping("/hello");
        tomcat.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println("Shutting down tomcat");
                    tomcat.stop();
                } catch (LifecycleException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}