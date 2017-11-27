package org.openeo.model.timeseries;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Arrays;
import java.util.Map;

import static be.vito.eodata.processing.MaskedStatisticsProcessor.StatisticsResult;

/**
 * A time series containing timestamped results.
 */
@XmlRootElement
@JsonSerialize(include = JsonSerialize.Inclusion.ALWAYS)
public class TimeSeriesResponse<T extends StatisticsResult> {
    private  TimestampedStatisticsResult<T>[] results;

    public TimeSeriesResponse(){}

    public TimeSeriesResponse(Map<String, T> statistics) {
        results = new TimestampedStatisticsResult[statistics.size()];
        int i=0;
        for (Map.Entry<String, T> e : statistics.entrySet()) {
            results[i++]= new TimestampedStatisticsResult<>(e.getKey(),e.getValue());
        }
    }

    public TimestampedStatisticsResult<T>[] getResults() {
        return results;
    }

    public void setResults(TimestampedStatisticsResult<T>[] results) {
        this.results = results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimeSeriesResponse<T> that = (TimeSeriesResponse<T>) o;

        if (!Arrays.equals(results, that.results)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return results != null ? Arrays.hashCode(results) : 0;
    }

    @Override
    public String toString() {
        return "TimeSeriesResponse{" +
                "results=" + Arrays.toString(results) +
                '}';
    }
}
