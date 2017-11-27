package org.openeo.model.timeseries;

import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import static be.vito.eodata.processing.MaskedStatisticsProcessor.StatisticsResult;

/**
 * A single timestamped entry of a timeseries.
 */
@XmlRootElement
@XmlType(name = "timestampedResultType")
public class TimestampedStatisticsResult<T extends StatisticsResult> {
    private String date;
    private T result;

    public TimestampedStatisticsResult() {
    }

    public TimestampedStatisticsResult(String date, T result) {
        this.date = date;
        this.result = result;
    }

    @XmlElementRef
    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TimestampedStatisticsResult<T> that = (TimestampedStatisticsResult<T>) o;

        if (!date.equals(that.date)) return false;
        return result.equals(that.result);

    }

    @Override
    public int hashCode() {
        int result1 = date.hashCode();
        result1 = 31 * result1 + result.hashCode();
        return result1;
    }
}