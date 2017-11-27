package org.openeo.driver.geotrellis;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("v0.1/jobs")
@Singleton
public class GeotrellisJobService {

    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public void submitJob(){

    }
}
