package oryanmoshe.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger log = LoggerFactory.getLogger(TimestampConverter.class);

    private static final Map<String, String> MONTH_MAP = new HashMap<>();

    static {
        MONTH_MAP.put("jan", "01");
        MONTH_MAP.put("feb", "02");
        MONTH_MAP.put("mar", "03");
        MONTH_MAP.put("apr", "04");
        MONTH_MAP.put("may", "05");
        MONTH_MAP.put("jun", "06");
        MONTH_MAP.put("jul", "07");
        MONTH_MAP.put("aug", "08");
        MONTH_MAP.put("sep", "09");
        MONTH_MAP.put("oct", "10");
        MONTH_MAP.put("nov", "11");
        MONTH_MAP.put("dec", "12");
    }

    public static final int MILLIS_LENGTH = 10;

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";

    public static final List<String> SUPPORTED_DATA_TYPES = Arrays.asList("date", "time", "datetime", "timestamp",
            "datetime2");

    private static final String DATETIME_REGEX = "(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?T?(?<time>(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))";
    private static final Pattern regexPattern = Pattern.compile(DATETIME_REGEX);

    public String strDatetimeFormat, strDateFormat, strTimeFormat;
    public Boolean debug;

    private SchemaBuilder datetimeSchema = SchemaBuilder.string().optional().name("oryanmoshe.time.DateTimeString");

    private DateTimeFormatter simpleDatetimeFormatter, simpleDateFormatter, simpleTimeFormatter;

    @Override
    public void configure(final Properties props) {
        this.strDatetimeFormat = props.getProperty("format.datetime", DEFAULT_DATETIME_FORMAT);
        this.simpleDatetimeFormatter = DateTimeFormatter.ofPattern(this.strDatetimeFormat);

        this.strDateFormat = props.getProperty("format.date", DEFAULT_DATE_FORMAT);
        this.simpleDateFormatter = DateTimeFormatter.ofPattern(this.strDateFormat);

        this.strTimeFormat = props.getProperty("format.time", DEFAULT_TIME_FORMAT);
        this.simpleTimeFormatter = DateTimeFormatter.ofPattern(this.strTimeFormat);

        this.debug = props.getProperty("debug", "false").equals("true");

        if (this.debug) {
            log.info("[TimestampConverter.configure] Finished configuring formats: this.strDatetimeFormat: {}, this.strTimeFormat: {}",
                this.strDatetimeFormat, this.strTimeFormat);
        }
    }

    @Override
    public void converterFor(final RelationalColumn column, final ConverterRegistration<SchemaBuilder> registration) {
        if (this.debug) {
            log.info("[TimestampConverter.converterFor] Starting to register column. column.name: {}, column.typeName: {}", column.name(),
                column.typeName());
        }

        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(column.typeName()))) {
            final boolean isTime = "time".equalsIgnoreCase(column.typeName());
            registration.register(datetimeSchema, rawValue -> {
                if (rawValue == null) {
                    return null;
                }

                final Long millis = getMillis(rawValue.toString(), isTime);
                if (millis == null) {
                    return rawValue.toString();
                }

                final Instant instant = Instant.ofEpochMilli(millis);
                final LocalDateTime dateObject = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);

                if (this.debug) {
                    log.info("[TimestampConverter.converterFor] Before returning conversion. column.name: {}, column.typeName: {}, millis: {}\n",
                        column.name(), column.typeName(), millis);
                }

                switch (column.typeName().toLowerCase()) {
                    case "time":
                        return this.simpleTimeFormatter.format(dateObject);
                    case "date":
                        return this.simpleDateFormatter.format(dateObject);
                    default:
                        return this.simpleDatetimeFormatter.format(dateObject);
                }
            });
        }
    }

    private Long getMillis(final String timestamp, final boolean isTime) {
        if (timestamp != null && timestamp.isEmpty()) {
            return null;
        }

        if (timestamp.contains(":") || timestamp.contains("-")) {
            return milliFromDateString(timestamp);
        }

        final int excessLength = timestamp.length() - MILLIS_LENGTH;
        final long longTimestamp = Long.parseLong(timestamp);

        if (isTime) {
            return longTimestamp;
        }

        if (excessLength < 0) {
            return longTimestamp * 24 * 60 * 60 * 1000;
        }

        if (excessLength == 3) {
            return longTimestamp;
        }

        final long millis = longTimestamp / (long) Math.pow(10, excessLength);

        return millis * 1000L;
    }

    private Long milliFromDateString(final String timestamp) {
        final Matcher matches = regexPattern.matcher(timestamp);

        if (matches.find()) {
            final String year = (matches.group("year") != null ? matches.group("year")
                    : (matches.group("year2") != null ? matches.group("year2") : matches.group("year3")));
            String month = (matches.group("month") != null ? matches.group("month")
                    : (matches.group("month2") != null ? matches.group("month2") : matches.group("month3")));
            final String day = (matches.group("day") != null ? matches.group("day")
                    : (matches.group("day2") != null ? matches.group("day2") : matches.group("day3")));
            final String hour = matches.group("hour") != null ? matches.group("hour") : "00";
            final String minute = matches.group("minute") != null ? matches.group("minute") : "00";
            final String second = matches.group("second") != null ? matches.group("second") : "00";
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

            final Date dateObj = Date.from(Instant.parse(dateStr));
            return dateObj.getTime();
        }

        return null;
    }
}
