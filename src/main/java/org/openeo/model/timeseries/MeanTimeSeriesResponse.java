package org.openeo.model.timeseries;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

import static be.vito.eodata.processing.MaskedStatisticsProcessor.StatsMeanResult;

@XmlRootElement
@JsonSerialize(include = JsonSerialize.Inclusion.ALWAYS)
public class MeanTimeSeriesResponse extends TimeSeriesResponse<StatsMeanResult> {

    public MeanTimeSeriesResponse(){}

    public MeanTimeSeriesResponse(Map<String, StatsMeanResult> statistics) {
        super(statistics);
    }
}
