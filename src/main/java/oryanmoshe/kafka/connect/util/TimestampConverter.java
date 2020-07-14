package oryanmoshe.kafka.connect.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final ImmutableMap<String, String> MONTH_MAP = ImmutableMap.<String, String>builder()
        .put("jan", "01")
        .put("feb", "02")
        .put("mar", "03")
        .put("apr", "04")
        .put("may", "05")
        .put("jun", "06")
        .put("jul", "07")
        .put("aug", "08")
        .put("sep", "09")
        .put("oct", "10")
        .put("nov", "11")
        .put("dec", "12")
        .build();
    public static final int MILLIS_LENGTH = 13;

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";

    public static final ImmutableList<String> SUPPORTED_DATA_TYPES = ImmutableList.of("date", "time", "datetime", "timestamp",
            "datetime2");

    private static final String DATETIME_REGEX = "(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?T?(?<time>(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))";
    private static final Pattern regexPattern = Pattern.compile(DATETIME_REGEX);

    public String strDatetimeFormat, strDateFormat, strTimeFormat;
    public Boolean debug;

    private SchemaBuilder datetimeSchema = SchemaBuilder.string().optional().name("oryanmoshe.time.DateTimeString");

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
                    "[TimestampConverter.configure] Finished configuring formats. this.strDatetimeFormat: %s, this.strTimeFormat: %s%n",
                    this.strDatetimeFormat, this.strTimeFormat);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (this.debug)
            System.out.printf(
                    "[TimestampConverter.converterFor] Starting to register column. column.name: %s, column.typeName: %s%n",
                    column.name(), column.typeName());
        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(column.typeName()))) {
            boolean isTime = "time".equalsIgnoreCase(column.typeName());
            registration.register(datetimeSchema, rawValue -> {
                if (rawValue == null)
                    return rawValue;

                Long millis = getMillis(rawValue.toString(), isTime);
                if (millis == null)
                    return rawValue.toString();

                Instant instant = Instant.ofEpochMilli(millis);
                Date dateObject = Date.from(instant);
                if (this.debug)
                    System.out.printf(
                            "[TimestampConverter.converterFor] Before returning conversion. column.name: %s, column.typeName: %s, millis: %d%n",
                            column.name(), column.typeName(), millis);
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

    private Long getMillis(String timestamp, boolean isTime) {
        if (timestamp.trim().isEmpty())
            return null;

        if (timestamp.contains(":") || timestamp.contains("-")) {
            return milliFromDateString(timestamp);
        }

        int excessLength = timestamp.length() - MILLIS_LENGTH;
        long longTimestamp = Long.parseLong(timestamp);

        if (isTime)
            return longTimestamp;

        if (excessLength < 0)
            return longTimestamp * 24 * 60 * 60 * 1000;

        long millis = longTimestamp / (long) Math.pow(10, excessLength);
        return millis;
    }

    private Long milliFromDateString(String timestamp) {
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