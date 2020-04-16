package com.rivery.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.kafka.connect.data.SchemaBuilder;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";
    public static final int MILLIS_LENGTH = 13;
    public static final List<String> SUPPORTED_DATA_TYPES = List.of("date", "time", "datetime", "timestamp");

    public String strDateFormat = DEFAULT_DATE_FORMAT;
    public String strTimeFormat = DEFAULT_TIME_FORMAT;
    public Boolean debug = false;

    private SchemaBuilder datetimeSchema = SchemaBuilder.string().optional().name("com.rivery.time.DateTimeString");

    private SimpleDateFormat simpleDateFormatter;
    private SimpleDateFormat simpleTimeFormatter;

    @Override
    public void configure(Properties props) {
        String propsDateFormat = props.getProperty("format.date");
        strDateFormat = propsDateFormat != null ? propsDateFormat : strDateFormat;
        simpleDateFormatter = new SimpleDateFormat(strDateFormat);

        String propsTimeFormat = props.getProperty("format.time");
        strTimeFormat = propsTimeFormat != null ? propsTimeFormat : strTimeFormat;
        simpleTimeFormatter = new SimpleDateFormat(strTimeFormat);

        String propsDebug = props.getProperty("debug");
        if (propsDebug != null && propsDebug.equals("true"))
            debug = true;

        simpleDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        simpleTimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        if (debug)
            System.out.printf(
                    "[TimestampConverter.configure] Finished configuring formats. strDateFormat: %s, strTimeFormat: %s%n",
                    strDateFormat, strTimeFormat);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (debug)
            System.out.printf(
                    "[TimestampConverter.converterFor] Starting to register column. column.name: %s, column.typeName: %s%n",
                    column.name(), column.typeName());
        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.toLowerCase().equals(column.typeName().toLowerCase()))) {
            boolean isTime = "time".equals(column.typeName().toLowerCase());
            registration.register(datetimeSchema, rawValue -> {
                Long millis = getMillis(rawValue.toString(), isTime);
                if (millis == null)
                    return rawValue.toString();

                Instant instant = Instant.ofEpochMilli(millis);
                Date dateObject = Date.from(instant);
                if (debug)
                    System.out.printf(
                            "[TimestampConverter.converterFor] Before returning conversion. column.name: %s, column.typeName: %s, millis: %d%n",
                            column.name(), column.typeName(), millis);
                if (isTime)
                    return simpleTimeFormatter.format(dateObject);
                return simpleDateFormatter.format(dateObject);
            });
        }
    }

    private Long getMillis(String timestamp, boolean isTime) {
        if (timestamp == null || timestamp.isBlank() || timestamp.contains(":"))
            return null;
        int excessLength = timestamp.length() - MILLIS_LENGTH;
        long longTimestamp = Long.parseLong(timestamp);

        if (isTime)
            return longTimestamp;

        if (excessLength < 0)
            return longTimestamp * 24 * 60 * 60 * 1000;

        long millis = longTimestamp / (long) Math.pow(10, excessLength);
        return millis;
    }
}