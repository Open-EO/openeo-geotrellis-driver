package org.openeo.driver.geotrellis;

import geotrellis.raster.Tile;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.SpaceTimeKey;
import geotrellis.spark.TileLayerMetadata;

import javax.inject.Singleton;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("v0.1/timeseries")
@Singleton
public class GeotrellisTimeseriesService {



    @POST
    @Path("/point")
    @Consumes({ MediaType.APPLICATION_JSON })
    @Produces(MediaType.APPLICATION_JSON)
    public String pointTimeSeries(@QueryParam("x") double x,@QueryParam("y") double y,@DefaultValue("EPSG:4326") @QueryParam("srs") String srs,JsonObject processGraph ){
        System.out.println("x = " + x);
        System.out.println("processGraph = " + processGraph);


        return "{'test':'test'}";
    }

    private ContextRDD<SpaceTimeKey,Tile,TileLayerMetadata<SpaceTimeKey>> graphToRdd(JsonObject processGraph) {
        String product = processGraph.getString("product_id");
        if (product != null) {
            return getProductRDD(product);
        }
        String processId = processGraph.getString("process_id");
        return getProcessRDD(processId, processGraph.getJsonObject("args"));

    }

    private ContextRDD<SpaceTimeKey,Tile,TileLayerMetadata<SpaceTimeKey>> getProcessRDD(String processId, JsonObject args) {

        return null;
    }

    private ContextRDD<SpaceTimeKey,Tile,TileLayerMetadata<SpaceTimeKey>> getProductRDD(String product) {
        return null;
    }
}
