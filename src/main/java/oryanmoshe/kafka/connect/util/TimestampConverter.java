package oryanmoshe.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Map<String, String> MONTH_MAP = Map.ofEntries(Map.entry("jan", "01"),
            Map.entry("feb", "02"),
            Map.entry("mar", "03"), Map.entry("apr", "04"), Map.entry("may", "05"),
            Map.entry("jun", "06"),
            Map.entry("jul", "07"), Map.entry("aug", "08"), Map.entry("sep", "09"),
            Map.entry("oct", "10"),
            Map.entry("nov", "11"), Map.entry("dec", "12"));
    public static final int MILLIS_LENGTH = 13;

    // columns of type "date" and "time" have special formats. All others are using the datetime format
    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";

    public static final List<String> SUPPORTED_DATA_TYPES = List.of("date", "time", "datetime", "timestamp",
            "datetime2");

    public static final String SUPPORTED_FORMATS = "[yyyy-MM-dd HH:mm:ss.SSSSSS]" + // ORDERING IS IMPORTANT
            "[yyyy-M-dd HH:mm:ss.SSSSSS]" +
            "[yyyy-M-dd H:m:s.SSSSSS]" +
            "[yyyy-MM-dd HH:mm:ss]" +
            "[yyyy-M-dd HH:mm:ss]" +
            "[dd/MM/yyyy HH:mm:ss.SSSSSS]" +
            "[dd-LLL-yyyy HH:mm:ss.SSSSSS]" +
            "[yyyy-M-dd HH:mm]" +
            "[yyyy-MM-dd]" +
            "[yyyy-M-dd]" +
            "[HH:mm:ss]";

    final DateTimeFormatter source_format =
            new DateTimeFormatterBuilder().appendPattern(SUPPORTED_FORMATS)
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter();

    public String strDatetimeFormat, strDateFormat, strTimeFormat;
    public Boolean debug;

    private SimpleDateFormat simpleDatetimeFormatter, simpleDateFormatter, simpleTimeFormatter;

    @Override
    public void configure(Properties props) {
        this.strDatetimeFormat = props.getProperty("format.datetime", DEFAULT_DATETIME_FORMAT);
        this.simpleDatetimeFormatter = new SimpleDateFormat(this.strDatetimeFormat);

        this.strDateFormat = props.getProperty("format.date", DEFAULT_DATE_FORMAT);
        this.simpleDateFormatter = new SimpleDateFormat(this.strDateFormat);

        this.strTimeFormat = props.getProperty("format.time", DEFAULT_TIME_FORMAT);
        this.simpleTimeFormatter = new SimpleDateFormat(this.strTimeFormat);

        this.debug = props.getProperty("debug", "false").equals("true");

        this.simpleDatetimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.simpleTimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        if (this.debug)
            System.out.printf(
                    "[TimestampConverter.configure] Finished configuring formats." +
                            " this.strDatetimeFormat: %s, this.strTimeFormat: %s%n",
                    this.strDatetimeFormat, this.strTimeFormat);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (this.debug)
            System.out.printf(
                    "[TimestampConverter.converterFor] Starting to register column. column.name: %s," +
                            " column.typeName: %s, column.hasDefaultValue: %s, column.defaultValue: %s," +
                            " column.isOptional: %s%n", column.name(), column.typeName(), column.hasDefaultValue(),
                    column.defaultValue(), column.isOptional());
        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(column.typeName()))) {
            boolean isTime = "time".equalsIgnoreCase(column.typeName());
            // Use a new SchemaBuilder every time in order to avoid changing "Already set" options
            // in the schema builder between tables.
            registration.register(SchemaBuilder.string().optional(), rawValue -> {
                if (rawValue == null) {
                    if (this.debug) {
                        System.out.printf("[TimestampConverter.converterFor] rawValue of %s is null.%n", column.name());
                    }

                    if (column.isOptional()) {
                        return null;
                    }
                    else if (column.hasDefaultValue()) {
                        return column.defaultValue();
                    }
                    return null;
                }

                String stringValue = rawValue.toString();

                if (this.debug)
                    System.out.printf(
                            "[TimestampConverter.converterFor] Before converting. rawValue: %s, isTime: %s",
                            stringValue, isTime);

                String result;
                try {
                    result = GetDateTimeFormat(stringValue, column);
                }
                catch (Exception e){
                    System.out.printf(
                            "[TimestampConverter.converterFor] ERROR! Using regex for conversion. rawValue: %s, " +
                                    "isTime: %s", stringValue, isTime);
                    // Using the legacy regex
                     long millis = milliFromDateString(stringValue);
                     result = convertMillisToDateTimeString(column, stringValue, millis);
                }
                return result;
            });
        }
    }

    private String GetDateTimeFormat(String rawValue, RelationalColumn column) {
        TemporalAccessor result = null;
        String output;

        try {
            result = source_format.parse(rawValue);
        } catch (DateTimeParseException dtpx) {
            // StringUtils is part of the apache commons lang3 library. Instead of strings,
            // we can always just filter by numeric value range
            if (StringUtils.isNumeric(rawValue)) {
                int length = rawValue.length();
                long numeric = Long.parseLong(rawValue);
                if (length == 5) { // Epoch Days
                    result = LocalDate.ofEpochDay(numeric);
                } else if (length == 10) { // Epoch Seconds
                    result = Instant.ofEpochSecond(numeric);
                    result = LocalDateTime.ofInstant((Instant) result, ZoneOffset.UTC);
                } else if (length == 13) { // Epoch Millis
                    result = Instant.ofEpochMilli(numeric);
                    result = LocalDateTime.ofInstant((Instant) result, ZoneOffset.UTC);
                } else if (column.typeName() == "time") { // time is milliseconds
                    Long nanoseconds = numeric * 1000000; // milli to nano
                    result = LocalTime.ofNanoOfDay(nanoseconds);
                }
            }
            if (result == null) {
                throw dtpx;
            }
        }
        SimpleDateFormat formatter = getFormatterPerColumnType(column);
        DateTimeFormatter target_format = DateTimeFormatter.ofPattern(formatter.toPattern(), Locale.ENGLISH);
        output = target_format.format(result);

        if (this.debug)
            System.out.printf("[TimestampConverter.GetDateTimeFormat] Final datetime string: %s", output);
        return output;
    }

    private String convertMillisToDateTimeString(RelationalColumn column, String rawValue, Long millis) {
        if (millis == null)
            return rawValue;

        Instant instant = Instant.ofEpochMilli(millis);
        Date dateObject = Date.from(instant);
        if (this.debug)
            System.out.printf(
                    "[TimestampConverter.converterFor] Before returning conversion. column.name: %s, column.typeName: %s, millis: %d%n",
                    column.name(), column.typeName(), millis);
        SimpleDateFormat formatter = getFormatterPerColumnType(column);
        return formatter.format(dateObject);
    }

    private SimpleDateFormat getFormatterPerColumnType(RelationalColumn column) {
        switch (column.typeName().toLowerCase()) {
            case "time":
                return this.simpleTimeFormatter;
            case "date":
                return this.simpleDateFormatter;
            default:
                return this.simpleDatetimeFormatter;
        }
    }

    // LEGACY METHOD
    private Long milliFromDateString(String timestamp) {
        // The regex is only for unknown patterns
        final String DATETIME_REGEX = "(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?T?(?<time>(?<hour>\\d{1,2}):?(?<minute>\\d{1,2}):?(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))";
        final Pattern regexPattern = Pattern.compile(DATETIME_REGEX);

        Matcher matches = regexPattern.matcher(timestamp);

        if (matches.find()) {
            String year = (matches.group("year") != null ? matches.group("year")
                    : (matches.group("year2") != null ? matches.group("year2") : matches.group("year3")));
            String month = (matches.group("month") != null ? matches.group("month")
                    : (matches.group("month2") != null ? matches.group("month2") : matches.group("month3")));
            String day = (matches.group("day") != null ? matches.group("day")
                    : (matches.group("day2") != null ? matches.group("day2") : matches.group("day3")));
            String hour = matches.group("hour") != null ? matches.group("hour") : "00";
            String minute = matches.group("minute") != null ? matches.group("minute") : "00";
            String second = matches.group("second") != null ? matches.group("second") : "00";
            String milli = matches.group("milli") != null ? matches.group("milli") : "000";

            if (milli.length() > 3)
                milli = milli.substring(0, 3);

            String dateStr = "";
            dateStr += String.format("%s:%s:%s.%s", ("00".substring(hour.length()) + hour),
                    ("00".substring(minute.length()) + minute), ("00".substring(second.length()) + second),
                    (milli + "000".substring(milli.length())));

            if (year != null) {
                if (month.length() > 2)
                    month = MONTH_MAP.get(month.toLowerCase());

                dateStr = String.format("%s-%s-%sT%sZ", year, ("00".substring(month.length()) + month),
                        ("00".substring(day.length()) + day), dateStr);
            } else {
                dateStr = String.format("%s-%s-%sT%sZ", "2020", "01", "01", dateStr);
            }

            Date dateObj = Date.from(Instant.parse(dateStr));
            return dateObj.getTime();
        }

        return null;
    }
}
