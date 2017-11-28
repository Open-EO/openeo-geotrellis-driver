package org.openeo.driver.geotrellis;

import be.vito.eodata.extracttimeseries.geotrellis.ComputeStatsGeotrellis;
import be.vito.eodata.extracttimeseries.geotrellis.MeanResult;
import be.vito.eodata.processing.MaskedStatisticsProcessor;
import geotrellis.raster.CellGrid;
import geotrellis.raster.MultibandTile;
import geotrellis.raster.Tile;
import geotrellis.spark.ContextRDD;
import geotrellis.spark.SpaceTimeKey;
import geotrellis.spark.TemporalKey;
import geotrellis.spark.TileLayerMetadata;
import geotrellis.spark.io.Intersects;
import geotrellis.spark.io.accumulo.AccumuloAttributeStore;
import geotrellis.spark.io.accumulo.AccumuloInstance;
import geotrellis.spark.io.accumulo.AccumuloInstance$;
import geotrellis.vector.Point;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.openeo.model.timeseries.MeanTimeSeriesResponse;
import org.openeo.model.timeseries.TimeSeriesResponse;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConverters;

import javax.inject.Singleton;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.text.ParsePosition;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalQuery;
import java.util.*;
import java.util.stream.Collectors;

@Path("v0.1/timeseries")
@Singleton
public class GeotrellisTimeseriesService {

    private ComputeStatsGeotrellis computeStatsGeotrellis = new ComputeStatsGeotrellis();

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

    @GET
    @Path("/layers")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getLayers() throws IOException {
        KerberosToken token = new KerberosToken();
        AccumuloInstance instance = AccumuloInstance$.MODULE$.apply("hdp-accumulo-instance", "epod6.vgt.vito.be:2181,epod17.vgt.vito.be:2181,epod1.vgt.vito.be:2181", token.getPrincipal(), token);
        AccumuloAttributeStore attributeStore = AccumuloAttributeStore.apply(instance);
        return JavaConverters.seqAsJavaListConverter(attributeStore.layerIds()).asJava().stream().map(id -> id.name()).collect(Collectors.toList());
    }

    @POST
    @Path("/point")
    @Consumes({ MediaType.APPLICATION_JSON })
    @Produces(MediaType.APPLICATION_JSON)
    public TimeSeriesResponse pointTimeSeries(
                                  @DefaultValue("") @QueryParam("startDate") String startDateStr,
                                  @DefaultValue("") @QueryParam("endDate") String endDateStr,
                                  @QueryParam("x") double x,
                                  @QueryParam("y") double y,
                                  @DefaultValue("EPSG:4326") @QueryParam("srs") String srs,

                                  JsonObject processGraph ){
        ViewParams viewingParameters = ViewParams.builder().bbox(Point.apply(x, y).buffer(0.01).envelope()).start(parseDateTime(startDateStr)).end(parseDateTime(endDateStr)).build();
        ContextRDD<SpaceTimeKey, Tile, TileLayerMetadata<SpaceTimeKey>> layer = graphToRdd(processGraph, viewingParameters);
        try{
            RDD<Tuple2<TemporalKey, MeanResult>> timeseries = computeStatsGeotrellis.computePointAverageTimeSeries(x, y, 1.0, 0.0, layer.rdd(), layer.metadata(), SparkContext.getOrCreate());
            return formatResponse(timeseries.toJavaRDD());
        }finally {
            SparkContext.getOrCreate().cleaner().get().registerRDDForCleanup(layer);
        }

    }

    private static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static TimeSeriesResponse formatResponse( // TODO rename to formatMeanReponse
                                                     JavaRDD<Tuple2<TemporalKey, MeanResult>> ts
    ) {
        SparkContext.getOrCreate().cleaner().get().registerRDDForCleanup(ts.rdd());
        List<Tuple2<TemporalKey, MeanResult>> tsResults = ts.collect();

        Map<String, MaskedStatisticsProcessor.StatsMeanResult> results = new TreeMap<>();
        for (Tuple2<TemporalKey, MeanResult> tsVal : tsResults) {
            MeanResult meanResult = tsVal._2();
            MaskedStatisticsProcessor.StatsMeanResult result = new MaskedStatisticsProcessor.StatsMeanResult(meanResult.meanPhysical(), meanResult.total(), meanResult.valid());
            results.put(dateFormat.format(tsVal._1().time()), result);
        }
        return new MeanTimeSeriesResponse(results);
    }

    private <V  extends CellGrid> ContextRDD<SpaceTimeKey,V,TileLayerMetadata<SpaceTimeKey>> graphToRdd(JsonObject processGraph, ViewParams viewingParameters) {

        if (processGraph.containsKey("product_id")) {
            return getProductRDD(processGraph.getString("product_id"),viewingParameters);
        } else if (processGraph.containsKey("process_id")) {
            String processId = processGraph.getString("process_id");
            return getProcessRDD(processId, processGraph.getJsonObject("args"),viewingParameters);
        }else{
            throw new IllegalArgumentException("Expected either 'product_id' or 'process_id' in: " + processGraph.toString());
        }
    }

    private <V  extends CellGrid> ContextRDD<SpaceTimeKey,V,TileLayerMetadata<SpaceTimeKey>> getProcessRDD(String processId, JsonObject args, ViewParams viewingParameters) {
        JsonObject imagery = args.getJsonObject("imagery");
        Objects.requireNonNull(imagery,"Required variable imagery should not be null in process: "+processId);
        switch (processId) {
            case "band_arithmetic":
                String function = args.getString("function");
                JsonArray bands = args.getJsonArray("bands");
                Objects.requireNonNull(function,"Required variable function should not be null in process: "+processId);
                Objects.requireNonNull(bands,"Required variable bands should not be null in process: "+processId);
                ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> imageryRDD = graphToRdd(imagery, viewingParameters);
                return (ContextRDD<SpaceTimeKey, V, TileLayerMetadata<SpaceTimeKey>>) bandArithmetic(bands,function, imageryRDD);
            default:
                throw new IllegalArgumentException("Unsupported process id: " + processId + " for Geotrellis backend.");
        }
    }

    private ContextRDD<SpaceTimeKey, Tile, TileLayerMetadata<SpaceTimeKey>> bandArithmetic(JsonArray bands, String function, ContextRDD<SpaceTimeKey, MultibandTile, TileLayerMetadata<SpaceTimeKey>> inputRDD) {
        return OpenEOGeotrellisHelper.bandArithmetic(bands, function, inputRDD);
    }

    private <V  extends CellGrid> ContextRDD<SpaceTimeKey,V,TileLayerMetadata<SpaceTimeKey>> getProductRDD(String product, ViewParams viewingParameters) {
        return (ContextRDD<SpaceTimeKey,V,TileLayerMetadata<SpaceTimeKey>>)computeStatsGeotrellis.readMultiBandLayer(product, viewingParameters.getStartDate().orElse(null), viewingParameters.getEndDate().orElse(null), Some.apply(Intersects.apply(viewingParameters.getBbox().orElse(null))), SparkContext.getOrCreate());
    }
}
