package org.openeo.driver.geotrellis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.spark.SparkContext;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.SimpleDateFormat;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class GeotrellisTimeseriesServiceTest extends JerseyTest {

    @BeforeClass
    public static void setupSpark() throws IOException {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.app.name", "Unit test: " + GeotrellisTimeseriesServiceTest.class.getSimpleName());
        //need to set up spark before configuring security, otherwise it will be reset
        SparkContext.getOrCreate();
        //do login
        String hadoopConfDir = System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.key());
        java.io.File confDir = new java.io.File(hadoopConfDir);
        Configuration conf = new HdfsConfiguration(true);
        conf.setClassLoader(new URLClassLoader(new URL[]{confDir.toURI().toURL()},Thread.currentThread().getContextClassLoader()));
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
    }

    @AfterClass
    public static void destroySpark() {

        SparkContext.getOrCreate().stop();
    }

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        return new ResourceConfig()//.register(LoggingFeature.class).property(JsonGenerator.PRETTY_PRINTING, true)
                .packages("org.openeo.driver.geotrellis")
                .register(JacksonFeature.class)
                .register(jacksonProvider());


                //.register(JsonProcessingFeature.class)

    }

    static JacksonJaxbJsonProvider jacksonProvider() {
        ObjectMapper mapper = new ObjectMapper();

        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));
        //mapper.disable(MapperFeature.AUTO_DETECT_CREATORS,  MapperFeature.AUTO_DETECT_FIELDS,
//                MapperFeature.AUTO_DETECT_GETTERS, MapperFeature.AUTO_DETECT_IS_GETTERS);
        mapper.registerModule(new JSR353Module());


        mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);

        JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
        provider.setMapper(mapper);

        return provider;
    }

    @Override
    protected void configureClient(ClientConfig config) {
        
        config.register(JacksonFeature.class)
            .register(jacksonProvider());
        //config.register(JsonProcessingFeature.class);
        //config.property(LoggingFeature.LOGGING_FEATURE_VERBOSITY_CLIENT, LoggingFeature.Verbosity.PAYLOAD_ANY)
        //        .property(LoggingFeature.LOGGING_FEATURE_LOGGER_LEVEL_CLIENT, "WARNING");
    }

    @Test
    public void testPointQuery() throws IOException {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);

        WebTarget target = target("v0.1");

        Response layersResponse = target.path("timeseries").path("layers").request(MediaType.APPLICATION_JSON).get();
        List<String> layers = layersResponse.readEntity(new GenericType<List<String>>() {
        });
        System.out.println("layersResponse = " + layers);
        Assert.assertTrue(layers.contains("SENTINEL2_RADIOMETRY"));
        ObjectNode job = JsonNodeFactory.instance.objectNode();


        job.put("product_id","test");
        job.put("int", 1);
        String jsonGraph = "{\n" +
                "            \"process_id\": \"band_arithmetic\",\n" +
                "            \"args\":\n" +
                "                {\n" +
                "                    \"imagery\":\n" +
                "                        {\n" +
                "                            \"product_id\": \"SENTINEL2_RADIOMETRY\"\n" +
                "                        },\n" +
                "                    \"bands\": [\"B0\", \"B1\", \"B2\"],\n" +
                "                    \"function\": \"gASVjQEAAAAAAACMF2Nsb3VkcGlja2xlLmNsb3VkcGlja2xllIwOX2ZpbGxfZnVuY3Rpb26Uk5QoaACMD19tYWtlX3NrZWxfZnVuY5STlGgAjA1fYnVpbHRpbl90eXBllJOUjAhDb2RlVHlwZZSFlFKUKEsDSwBLA0sCS1NDCHwAfAEXAFMAlE6FlCmMBWJhbmQxlIwFYmFuZDKUjAViYW5kM5SHlIxFL2hvbWUvZHJpZXNqL3B5dGhvbndvcmtzcGFjZS9vcGVuZW8tY2xpZW50LWFwaS90ZXN0cy90ZXN0X2JhbmRtYXRoLnB5lIwIPGxhbWJkYT6USyBDAJQpKXSUUpRK/////32Uh5RSlH2UKIwHZ2xvYmFsc5R9lIwIZGVmYXVsdHOUTowEZGljdJR9lIwGbW9kdWxllIwNdGVzdF9iYW5kbWF0aJSMDmNsb3N1cmVfdmFsdWVzlE6MCHF1YWxuYW1llIwoVGVzdEJhbmRNYXRoLnRlc3RfbmR2aS48bG9jYWxzPi48bGFtYmRhPpR1dFIu\"\n" +
                "                }\n" +
                "        }";
        JsonObject jsonObject = Json.createReader(new StringReader(jsonGraph)).readObject();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JSR353Module());
        JsonObject graphObject = mapper.readValue(jsonGraph,JsonObject.class);
        System.out.println("graphObject = " + graphObject);
        //Map entity = mapper.convertValue(graphObject, Map.class);


        Response responseMsg = target
                .path("timeseries")
                .path("point")
                .queryParam("y","51.1450000")
                .queryParam("x","4.9650000")
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(jsonObject));
        System.out.println("responseMsg = " + responseMsg.getStatus());
        //System.out.println("this.getLastLoggedRecord() = " + this.getLastLoggedRecord());
        assertEquals("{\"results\":[{\"date\":\"2017-10-03\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.04830917874396135}},{\"date\":\"2017-10-05\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.014692653673163419}},{\"date\":\"2017-10-08\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.005182092989779761}},{\"date\":\"2017-10-10\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":-0.008001196440589248}},{\"date\":\"2017-10-13\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.020606060606060607}},{\"date\":\"2017-10-15\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.20631067961165048}},{\"date\":\"2017-10-18\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.22822491730981256}},{\"date\":\"2017-10-20\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":-0.015339124006653115}}]}", responseMsg.readEntity(String.class));
    }

}