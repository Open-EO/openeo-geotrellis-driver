package org.openeo.driver.geotrellis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.Test;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class GeotrellisTimeseriesServiceTest extends JerseyTest {

    @Override
    protected Application configure() {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        return new ResourceConfig().register(LoggingFeature.class).property(JsonGenerator.PRETTY_PRINTING, true)
                .register(JacksonFeature.class)
                .register(jacksonProvider())
                //.register(JsonProcessingFeature.class)
                .packages("org.openeo.driver.geotrellis","org.codehaus.jackson.jaxrs");
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
        
        config.register(JacksonFeature.class);
        //config.register(JsonProcessingFeature.class);
        config.property(LoggingFeature.LOGGING_FEATURE_VERBOSITY_CLIENT, LoggingFeature.Verbosity.PAYLOAD_ANY)
                .property(LoggingFeature.LOGGING_FEATURE_LOGGER_LEVEL_CLIENT, "WARNING");
    }

    @Test
    public void testPointQuery() throws IOException {
        enable(TestProperties.LOG_TRAFFIC);
        enable(TestProperties.DUMP_ENTITY);
        ObjectNode job = JsonNodeFactory.instance.objectNode();
        Map<String, Object> data = new HashMap<>();
        data.put("product_id", "SENTINEL2");
        data.put("int", "1");
        job.put("product_id","test");
        job.put("int", 1);
        String jsonGraph = "{\n" +
                "            \"process_id\": \"band_arithmetic\",\n" +
                "            \"args\":\n" +
                "                {\n" +
                "                    \"imagery\":\n" +
                "                        {\n" +
                "                            \"product_id\": \"SENTINEL2_RADIOMETRY_10M\"\n" +
                "                        },\n" +
                "                    \"bands\": [\"B0\", \"B1\", \"B2\"],\n" +
                "                    \"function\": \"gASVjQEAAAAAAACMF2Nsb3VkcGlja2xlLmNsb3VkcGlja2xllIwOX2ZpbGxfZnVuY3Rpb26Uk5QoaACMD19tYWtlX3NrZWxfZnVuY5STlGgAjA1fYnVpbHRpbl90eXBllJOUjAhDb2RlVHlwZZSFlFKUKEsDSwBLA0sCS1NDCHwAfAEXAFMAlE6FlCmMBWJhbmQxlIwFYmFuZDKUjAViYW5kM5SHlIxFL2hvbWUvZHJpZXNqL3B5dGhvbndvcmtzcGFjZS9vcGVuZW8tY2xpZW50LWFwaS90ZXN0cy90ZXN0X2JhbmRtYXRoLnB5lIwIPGxhbWJkYT6USyBDAJQpKXSUUpRK/////32Uh5RSlH2UKIwHZ2xvYmFsc5R9lIwIZGVmYXVsdHOUTowEZGljdJR9lIwGbW9kdWxllIwNdGVzdF9iYW5kbWF0aJSMDmNsb3N1cmVfdmFsdWVzlE6MCHF1YWxuYW1llIwoVGVzdEJhbmRNYXRoLnRlc3RfbmR2aS48bG9jYWxzPi48bGFtYmRhPpR1dFIu\"\n" +
                "                }\n" +
                "        }";
        JsonObject jsonObject = Json.createReader(new StringReader(jsonGraph)).readObject();
        //ObjectMapper mapper = new ObjectMapper();
        //JsonNode graphObject = mapper.readTree(jsonGraph);
        //Map entity = mapper.convertValue(graphObject, Map.class);
        WebTarget target = target("v0.1");

        Response responseMsg = target
                .path("timeseries")
                .path("point")
                .queryParam("y","51.1450000")
                .queryParam("x","4.9650000")
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(jsonObject));
        System.out.println("responseMsg = " + responseMsg.getStatus());
        //System.out.println("this.getLastLoggedRecord() = " + this.getLastLoggedRecord());
        assertEquals("{\"results\":[{\"date\":\"2016-07-03\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}},{\"date\":\"2016-07-07\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}},{\"date\":\"2016-07-10\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.3799999952316284}},{\"date\":\"2016-07-13\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}},{\"date\":\"2016-07-14\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}},{\"date\":\"2016-07-17\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}},{\"date\":\"2016-07-20\",\"result\":{\"totalCount\":1,\"validCount\":1,\"average\":0.7649999856948853}},{\"date\":\"2016-07-24\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}},{\"date\":\"2016-07-27\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}},{\"date\":\"2016-07-30\",\"result\":{\"totalCount\":1,\"validCount\":0,\"average\":\"NaN\"}}]}", responseMsg.readEntity(String.class));
    }

}