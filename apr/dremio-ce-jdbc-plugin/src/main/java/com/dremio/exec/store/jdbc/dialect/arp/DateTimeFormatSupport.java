package com.dremio.exec.store.jdbc.dialect.arp;

import java.util.*;
import com.fasterxml.jackson.annotation.*;
import com.google.common.collect.*;

public class DateTimeFormatSupport
{
    private final List<DateTimeFormatMapping> dateTimeFormatMappings;
    
    DateTimeFormatSupport(@JsonProperty("era") final DateTimeFormatDescriptor era, @JsonProperty("meridian") final DateTimeFormatDescriptor meridian, @JsonProperty("century") final DateTimeFormatDescriptor century, @JsonProperty("week_of_year") final DateTimeFormatDescriptor week_of_year, @JsonProperty("day_of_week") final DateTimeFormatDescriptor day_of_week, @JsonProperty("day_name_abbreviated") final DateTimeFormatDescriptor day_name_abbreviated, @JsonProperty("day_name") final DateTimeFormatDescriptor day_name, @JsonProperty("year_4") final DateTimeFormatDescriptor year_4, @JsonProperty("year_2") final DateTimeFormatDescriptor year_2, @JsonProperty("day_of_year") final DateTimeFormatDescriptor day_of_year, @JsonProperty("month") final DateTimeFormatDescriptor month, @JsonProperty("month_name_abbreviated") final DateTimeFormatDescriptor month_name_abbreviated, @JsonProperty("month_name") final DateTimeFormatDescriptor month_name, @JsonProperty("day_of_month") final DateTimeFormatDescriptor day_of_month, @JsonProperty("hour_12") final DateTimeFormatDescriptor hour_12, @JsonProperty("hour_24") final DateTimeFormatDescriptor hour_24, @JsonProperty("minute") final DateTimeFormatDescriptor minute, @JsonProperty("second") final DateTimeFormatDescriptor second, @JsonProperty("millisecond") final DateTimeFormatDescriptor millisecond, @JsonProperty("timezone_abbreviation") final DateTimeFormatDescriptor timezone_abbreviation, @JsonProperty("timezone_offset") final DateTimeFormatDescriptor timezone_offset) {
        this.dateTimeFormatMappings = (List<DateTimeFormatMapping>)new ImmutableList.Builder().add(new DateTimeFormatMapping("DDD", day_of_year)).add(new DateTimeFormatMapping("DD", day_of_month)).add(new DateTimeFormatMapping("DAY", day_name)).add(new DateTimeFormatMapping("DY", day_name_abbreviated)).add(new DateTimeFormatMapping("YYYY", year_4)).add(new DateTimeFormatMapping("YY", year_2)).add(new DateTimeFormatMapping("AD", era)).add(new DateTimeFormatMapping("BC", era)).add(new DateTimeFormatMapping("CC", century)).add(new DateTimeFormatMapping("WW", week_of_year)).add(new DateTimeFormatMapping("MONTH", month_name)).add(new DateTimeFormatMapping("MON", month_name_abbreviated)).add(new DateTimeFormatMapping("MM", month)).add(new DateTimeFormatMapping("HH24", hour_24)).add(new DateTimeFormatMapping("HH12", hour_12)).add(new DateTimeFormatMapping("HH", hour_12)).add(new DateTimeFormatMapping("MI", minute)).add(new DateTimeFormatMapping("SS", second)).add(new DateTimeFormatMapping("AM", meridian)).add(new DateTimeFormatMapping("PM", meridian)).add(new DateTimeFormatMapping("FFF", millisecond)).add(new DateTimeFormatMapping("TZD", timezone_abbreviation)).add(new DateTimeFormatMapping("TZO", timezone_offset)).add(new DateTimeFormatMapping("D", day_of_week)).build();
    }
    
    public List<DateTimeFormatMapping> getDateTimeFormatMappings() {
        return this.dateTimeFormatMappings;
    }
    
    static class DateTimeFormatMapping
    {
        private final String dremioDateTimeFormatString;
        private final DateTimeFormatDescriptor sourceDateTimeFormat;
        private final boolean areFormatsEqual;
        
        DateTimeFormatMapping(final String format, final DateTimeFormatDescriptor mapping) {
            this.dremioDateTimeFormatString = format;
            this.sourceDateTimeFormat = mapping;
            this.areFormatsEqual = (mapping != null && format.equals(mapping.getFormat()));
        }
        
        public String getDremioDateTimeFormatString() {
            return this.dremioDateTimeFormatString;
        }
        
        public DateTimeFormatDescriptor getSourceDateTimeFormat() {
            return this.sourceDateTimeFormat;
        }
        
        public boolean areDateTimeFormatsEqual() {
            return this.areFormatsEqual;
        }
    }
}
