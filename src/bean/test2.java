package bean;
//import java.sql.*;

//import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
//import javax.ws.rs.POST;
import javax.ws.rs.Path;
//import javax.ws.rs.Produces;
//import javax.ws.rs.core.MediaType;

//import org.codehaus.jackson.map.ObjectMapper;
//import java.io.*;

@Path("/fciServerT")
public class test2 {
	@GET
    @Path("ping")
    public String getServerTime() {
        System.out.println("FCI API is up and running");
        return "FCI API is up and running :xxx ";
    }	
}