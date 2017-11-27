package org.openeo.driver.geotrellis;

import geotrellis.vector.Extent;

import javax.validation.constraints.NotNull;
import java.time.ZonedDateTime;
import java.util.Optional;

public class ViewParams {
    private final Optional<Extent> bbox;
    private final Optional<ZonedDateTime> startDate;
    private final Optional <ZonedDateTime> endDate;

    public ViewParams(Extent bbox, Optional<ZonedDateTime> startDate, Optional<ZonedDateTime> endDate) {
        this.bbox = Optional.ofNullable(bbox);
        this.startDate = startDate;
        this.endDate = endDate;
    }

    @NotNull
    public Optional<Extent> getBbox() {
        return bbox;
    }

    @NotNull
    public Optional<ZonedDateTime> getStartDate() {
        return startDate;
    }

    @NotNull
    public Optional<ZonedDateTime> getEndDate() {
        return endDate;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder{

        private Extent bbox;
        private Optional<ZonedDateTime> startDate;
        private Optional<ZonedDateTime> endDate;

        public Builder() {
        }

        public Builder bbox(Extent bbox) {
            this.bbox = bbox;
            return this;
        }

        public Builder start(ZonedDateTime time){
            this.startDate = Optional.ofNullable(time);
            return this;
        }

        public Builder end(ZonedDateTime time){
            this.endDate = Optional.ofNullable(time);
            return this;
        }

        public Builder start(Optional<ZonedDateTime> time){
            this.startDate = time;
            return this;
        }

        public Builder end(Optional<ZonedDateTime> time){
            this.endDate = time;
            return this;
        }

        public ViewParams build(){
            return new ViewParams(bbox,startDate,endDate);
        }
    }
}
