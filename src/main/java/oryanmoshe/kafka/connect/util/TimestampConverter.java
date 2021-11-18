package oryanmoshe.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.SchemaBuilder;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Map<String, String> MONTH_MAP = Map.ofEntries(Map.entry("jan", "01"), Map.entry("feb", "02"),
            Map.entry("mar", "03"), Map.entry("apr", "04"), Map.entry("may", "05"), Map.entry("jun", "06"),
            Map.entry("jul", "07"), Map.entry("aug", "08"), Map.entry("sep", "09"), Map.entry("oct", "10"),
            Map.entry("nov", "11"), Map.entry("dec", "12"));
    public static final int MILLIS_LENGTH = 13;

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";

    public static final List<String> SUPPORTED_DATA_TYPES = List.of("date", "time", "datetime", "timestamp",
            "datetime2");

    private static final String DATETIME_REGEX = "(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?T?(?<time>(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))";
    private static final Pattern regexPattern = Pattern.compile(DATETIME_REGEX);

    public Boolean debug;

    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter dateFormatter;
    private SimpleDateFormat timeFormatter;

    @Override
    public void configure(Properties props) {
        final String dateTimeFormatter = props.getProperty("format.datetime", DEFAULT_DATETIME_FORMAT);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatter).withZone(ZoneOffset.UTC);

        final String dateFormatter = props.getProperty("format.date", DEFAULT_DATE_FORMAT);
        this.dateFormatter = DateTimeFormatter.ofPattern(dateFormatter).withZone(ZoneOffset.UTC);

        final String timeFormatter = props.getProperty("format.time", DEFAULT_TIME_FORMAT);
        this.timeFormatter = new SimpleDateFormat(timeFormatter);

        this.debug = props.getProperty("debug", "false").equals("true");

        this.timeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        if (this.debug) {
            System.out.printf(
                    "[TimestampConverter.configure] dateTimeFormatter: %s, dateFormatter: %s, timeFormatter: %s%n",
                    dateTimeFormatter, dateFormatter, timeFormatter);
        }
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (this.debug)
            System.out.printf(
                    "[TimestampConverter.converterFor] Starting to register column. column.name: %s, column.typeName: %s, column.hasDefaultValue: %s, column.defaultValue: %s, column.isOptional: %s%n",
                    column.name(), column.typeName(), column.hasDefaultValue(), column.defaultValue(), column.isOptional());
        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(column.typeName()))) {
            // Use a new SchemaBuilder every time in order to avoid changing "Already set" options
            // in the schema builder between tables.

            // Building the schema for the payload
            final SchemaBuilder builder = column.isOptional() ? SchemaBuilder.string().optional() : SchemaBuilder.string().required();
            registration.register(builder, rawValue -> {
                if (rawValue == null) {
                    if (this.debug) { System.out.printf("[TimestampConverter.converterFor] rawValue of %s is null.%n", column.name()); }

                    if (column.isOptional()) { return null; }
                    else if (column.hasDefaultValue()) { return column.defaultValue(); }
                    return null;
                }

                final Long epoch = parseToEpoch(rawValue.toString());

                if (this.debug) { System.out.printf("[TimestampConverter.converterFor] Parsed epoch: %d%n", epoch); }
                if (epoch == null) { return rawValue.toString(); }

                final Instant instant = Instant.EPOCH.plus(epoch, ChronoUnit.MICROS);

                if (this.debug) {
                    System.out.printf(
                            "[TimestampConverter.converterFor] Before returning conversion. column.name: %s, column.typeName: %s, epoch: %d%n",
                            column.name(), column.typeName(), epoch);
                }
                switch (column.typeName().toLowerCase()) {
                    case "time":
                        if (this.debug) { System.out.println("Using timeFormatter"); }
                        return this.timeFormatter.format(instant);
                    case "date":
                        if (this.debug) { System.out.println("Using dateFormatter"); }
                        return this.dateFormatter.format(instant);
                    default:
                        if (this.debug) { System.out.println("Using dateTimeFormatter"); }
                        return this.dateTimeFormatter.format(instant);
                }
            });
        }
    }

    private Long parseToEpoch(final String timestamp) {
        if (timestamp == null || timestamp.isBlank()) { return null; }
        if (timestamp.contains(":") || timestamp.contains("-")) {
            return epochFromDateString(timestamp);
        }
        return Long.parseLong(timestamp);
    }

    private Long epochFromDateString(final String timestamp) {
        System.out.println("TIMESTAMP: " + timestamp);

//        final Instant instant = LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")).atZone(ZoneOffset.UTC).toInstant();
//        return Long.parseLong(String.valueOf(instant.getEpochSecond()) + instant.getLong(ChronoField.MICRO_OF_SECOND));

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

            String dateStr = "";
            dateStr += String.format("%s:%s:%s.%s", ("00".substring(hour.length()) + hour),
                    ("00".substring(minute.length()) + minute), ("00".substring(second.length()) + second),
                    (milli + "000000".substring(milli.length())));

            if (year != null) {
                if (month.length() > 2)
                    month = MONTH_MAP.get(month.toLowerCase());

                dateStr = String.format("%s-%s-%sT%sZ", year, ("00".substring(month.length()) + month),
                        ("00".substring(day.length()) + day), dateStr);
            } else {
                dateStr = String.format("%s-%s-%sT%sZ", "2020", "01", "01", dateStr);
            }

            final Instant instant = Instant.parse(dateStr);

            return Long.parseLong(String.valueOf(instant.getEpochSecond()) + instant.getLong(ChronoField.MICRO_OF_SECOND));
        }
        return null;
    }
}
