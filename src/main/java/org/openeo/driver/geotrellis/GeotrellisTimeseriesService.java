package org.openeo.driver.geotrellis;

import be.vito.eodata.extracttimeseries.geotrellis.ComputeStatsGeotrellis;
import geotrellis.raster.Tile;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.SpaceTimeKey;
import geotrellis.spark.TileLayerMetadata;
import geotrellis.spark.io.Intersects;
import geotrellis.vector.Point;
import org.apache.spark.SparkContext;
import scala.Some;

import javax.inject.Singleton;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.text.ParsePosition;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalQuery;
import java.util.Objects;
import java.util.Optional;

@Path("v0.1/timeseries")
@Singleton
public class GeotrellisTimeseriesService {

    private final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private Optional<ZonedDateTime> parseDateTime(String dateStr) {
        if (dateStr.length() == 0) {
            return Optional.empty();
        }
        return Optional.of(parseFirstMatching(dateStr, ZonedDateTime::from, DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ISO_DATE));
    }

    public static <T> T parseFirstMatching(CharSequence text, TemporalQuery<T> query, DateTimeFormatter... formatters) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(formatters, "formatters");
        if (formatters.length == 0) {
            throw new DateTimeParseException("No formatters specified", text, 0);
        }
        if (formatters.length == 1) {
            return formatters[0].parse(text, query);
        }
        for (DateTimeFormatter formatter : formatters) {
            try {
                ParsePosition pp = new ParsePosition(0);
                formatter.parseUnresolved(text, pp);
                int len = text.length();
                if (pp.getErrorIndex() == -1 && pp.getIndex() == len) {
                    return formatter.parse(text, query);
                }
            } catch (RuntimeException ex) {
                // should not happen, but ignore if it does
            }
        }
        throw new DateTimeParseException("Text '" + text + "' could not be parsed", text, 0);
    }


    @POST
    @Path("/point")
    @Consumes({ MediaType.APPLICATION_JSON })
    @Produces(MediaType.APPLICATION_JSON)
    public String pointTimeSeries(
                                  @DefaultValue("") @QueryParam("startDate") String startDateStr,
                                  @DefaultValue("") @QueryParam("endDate") String endDateStr,
                                  @QueryParam("x") double x,
                                  @QueryParam("y") double y,
                                  @DefaultValue("EPSG:4326") @QueryParam("srs") String srs,

                                  JsonObject processGraph ){
        System.out.println("x = " + x);
        System.out.println("processGraph = " + processGraph);



        ViewParams viewingParameters = ViewParams.builder().bbox(Point.apply(x, y).buffer(0.01).envelope()).start(parseDateTime(startDateStr)).end(parseDateTime(endDateStr)).build();
        ContextRDD<SpaceTimeKey, Tile, TileLayerMetadata<SpaceTimeKey>> layer = graphToRdd(processGraph, viewingParameters);
        try{

        }finally {
            SparkContext.getOrCreate().cleaner().get().registerRDDForCleanup(layer);
        }
        return "{'test':'test'}";
    }

    private ContextRDD<SpaceTimeKey,Tile,TileLayerMetadata<SpaceTimeKey>> graphToRdd(JsonObject processGraph, ViewParams viewingParameters) {

        String product = processGraph.getString("product_id");
        if (product != null) {
            return getProductRDD(product,viewingParameters);
        }
        String processId = processGraph.getString("process_id");
        return getProcessRDD(processId, processGraph.getJsonObject("args"));

    }

    private ContextRDD<SpaceTimeKey,Tile,TileLayerMetadata<SpaceTimeKey>> getProcessRDD(String processId, JsonObject args) {

        return null;
    }

    private ContextRDD<SpaceTimeKey,Tile,TileLayerMetadata<SpaceTimeKey>> getProductRDD(String product, ViewParams viewingParameters) {
        return new ComputeStatsGeotrellis().readMultiBandLayer(product, viewingParameters.getStartDate().get(), viewingParameters.getEndDate().get(), Some.apply(Intersects.apply(viewingParameters.getBbox().get())), SparkContext.getOrCreate());
    }
}
