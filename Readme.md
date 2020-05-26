# UFO Sightings

## Overview
- This project performs Apache Spark RDD transformation/actions on a csv file that details 83,000 UFO sightings. It displays the results on a webpage ran on en embedded Tomcat webserver.

## Technologies
- Apache Spark, to convert the csv file into an Spark RDD and perform transformations on it
- Amazon AWS EC2 instance, to store the data in a PostreSQL database
- Docker, to create a container that runs the PostreSQL database
- Embedded Tomcat server, to create a web server. The proper HTTP GET requests sent to this server will 


## To run embedded tomcat server
- java -jar target/spark-0.0.1-SNAPSHOT.jar

## List of Servlets

- ByCountryServlet
    - mapping: country
        - List of Commands: 
        - ?inputType=inCountry&country=(yourcountryhere)    
        - ?inputType=byCountry
    - ex: localhost:8080/spark/country?inputType=byCountry

- ByDurationServlet
    - mapping: duration
    - ex: localhost:8080/spark/duration

- ByShapeServlet
    - mapping: shape
    - ex: localhost:8080/spark/shape

- ByStateServlet
    - mapping: state
        - List of Commands:
        - ?inputType=inState&State=(yourcountryhere)    
        - ?inputType=byState
    - ex: localhost:8080/spark/state?inputType=inState&State=us

- ByTimeServlet
    - mapping: time
        - ?inputType=byHour
        - ?inputType=byMonth
        - ?inputType=byYear
    - ex: localhost:8080/spark/time?inputType=byHour





