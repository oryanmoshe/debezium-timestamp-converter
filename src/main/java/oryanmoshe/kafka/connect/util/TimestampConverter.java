package oryanmoshe.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.*;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.data.SchemaBuilder;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    public InternalConverter getConverter(int _convert_mode) {

	InternalConverter conv = null;

	switch(_convert_mode) {
	case InternalConverter.CONVERT_MODE_POSTGRESQL:
	    conv = new PostgreSQLTimestampConverter();
	    break;
	default:
	    conv = new GenericTimestampConverter();
	    break;
	}

	return conv;
    }

    public class InternalConverter {

	public final Map<String, String> MONTH_MAP = Map.ofEntries(Map.entry("jan", "01"), Map.entry("feb", "02"),
								   Map.entry("mar", "03"), Map.entry("apr", "04"), Map.entry("may", "05"), Map.entry("jun", "06"),
								   Map.entry("jul", "07"), Map.entry("aug", "08"), Map.entry("sep", "09"), Map.entry("oct", "10"),
								   Map.entry("nov", "11"), Map.entry("dec", "12"));

	public static final int MILLIS_LENGTH = 13;

	public static final int CONVERT_MODE_GENERIC = 0;
	public static final int CONVERT_MODE_POSTGRESQL  = 1;
	public RelationalColumn column;

	protected Long milliFromDateString(String timestamp) {
	    return new Long(0);
	}

	protected Long getMillis(String timestamp, boolean isTime) {

	    if (timestamp.isBlank())
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

	public String run(String raw_value, boolean isTime) {
	    return raw_value;
	}

	public InternalConverter() {

	    column = null;

	}

	public String getTimeZoneID() {
	    return tzstring;
	}

	public void printAvailableTimeZones() {

	    logger.info("[TimestampConverter.printAvailableTimeTones] no timezones supported by current converter");

	}

    }

    public class GenericTimestampConverter extends InternalConverter {

	private final String DATETIME_REGEX = "(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?T?(?<time>(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))";
	private final Pattern regexPattern = Pattern.compile(DATETIME_REGEX);
	private SimpleDateFormat simpleDatetimeFormatter, simpleDateFormatter, simpleTimeFormatter;
	public TimeZone tz;

	public String getTimeZoneID() {

	    if (tz != null) {
		return this.tz.getID();
	    } else {
		return "";
	    }

	}

	public void printAvailableTimeZones() {

	    String tzids[] = TimeZone.getAvailableIDs();

	    for(int i = 0; i < tzids.length; i++) {

		logger.info("[TimestampConverter.printAvailableTimeZones] TimeZone ID {}", tzids[i]);
	    }

	}

	public String run(String rawValue, boolean isTime) {

	    String convertedValue = rawValue;;

	    Long millis = getMillis(rawValue.toString(), isTime);
	    if (millis == null)
		return rawValue.toString();

	    Instant instant = Instant.ofEpochMilli(millis);
	    Date dateObject = Date.from(instant);

	    if (debug)
		logger.info("[TimestampConverter.converterFor] Before returning conversion. column.name: {}, column.typeName: {}, millis: {}",
			    column.name(), column.typeName(), millis);

	    switch (column.typeName().toLowerCase()) {
	    case "timetz":
	    case "time":
		convertedValue = this.simpleTimeFormatter.format(dateObject);
		break;
	    case "date":
		convertedValue = this.simpleDateFormatter.format(dateObject);
		break;
	    default:
		convertedValue = this.simpleDatetimeFormatter.format(dateObject);
		break;
	    }

	    if (debug)
		logger.info("[TimestampConverter.converterFor] After conversion. column.name: {}, column.typeName: {}, convertedValue: {}",
			    column.name(), column.typeName(), convertedValue);

	    return convertedValue;

	}

	public GenericTimestampConverter() {

	    super();
	    simpleDatetimeFormatter = new SimpleDateFormat(strDatetimeFormat);
	    simpleDateFormatter = new SimpleDateFormat(strDateFormat);
	    simpleTimeFormatter = new SimpleDateFormat(strTimeFormat);

	    tz = TimeZone.getTimeZone(tzstring);
	    simpleDatetimeFormatter.setTimeZone(tz);
	    simpleTimeFormatter.setTimeZone(tz);

	}

	protected Long milliFromDateString(String timestamp) {

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

		if (debug)
		    logger.info("[TimestampConverter.milliFromDateString] decoded dateStr = {}", dateStr);

		Date dateObj = Date.from(Instant.parse(dateStr));
		return dateObj.getTime();

	    }

	    return null;
	}

    }

    public class PostgreSQLTimestampConverter extends InternalConverter {

	public void printAvailableTimeZones() {

	    Set<String> tzids = ZoneId.getAvailableZoneIds();
	    Iterator <String> it = tzids.iterator();

	    while(it.hasNext()) {

		logger.info("[TimestampConverter.printAvailableTimeZones] TimeZone ID {}", it.next());
	    }

	}

	public String getTimeZoneID() {
	    return tzstring;
	}

	public PostgreSQLTimestampConverter() {

	    super();

	}

	public String convertDate(String val, boolean isTime) {

	    LocalDate ld = LocalDate.parse(val, pgDateInputFormatter);
	    return DateTimeFormatter.ofPattern(strDateFormat).format(ld);

	}

	public String convertTime(String val) {

	    String result = val;

	    try {

		LocalTime lt = LocalTime.parse(val, pgTimeInputFormatter);
		result = DateTimeFormatter.ofPattern(strTimeFormat).format(lt);

	    } catch(DateTimeParseException e) {

		logger.error("[TimestampConverter.convertTime] could not convert value\"{}\", aborting conversion with error \"{}\"",
			     val, e.getMessage());

	    }

	    return result;

	}

	public String convertTimestamp(String val) {

	    String result = val;

	    try {

		LocalDateTime ldt = LocalDateTime.parse(val, pgTimestampInputFormatter);
		result = DateTimeFormatter.ofPattern(strDatetimeFormat).format(ldt);

	    } catch(DateTimeParseException e) {

		logger.error("[TimestampConverter.convertTimestamp] could not convert value\"{}\", aborting conversion with error \"{}\"",
			     val, e.getMessage());

	    }

	    return result;

	}

	public String convertTimestampTz(String val) {

	    String result = val;

	    try {

		LocalDateTime ldt = LocalDateTime.parse(val, pgTimestampInputFormatter);
		ZoneId utc = ZoneId.of("UTC");
		ZoneId zid = ZoneId.of(tzstring);

		if (debug)
		    logger.info("[TimestampConverter.convertTimestampTz] using conversion time zone \"{}\"", zid.getId());

		ZonedDateTime zt = ldt.atZone(utc);
		ZonedDateTime zt_tz = zt.withZoneSameInstant(zid);
		DateTimeFormatter result_formatter = DateTimeFormatter.ofPattern(strDatetimeFormat);
		result = result_formatter.format(zt_tz);

	    } catch (DateTimeParseException e) {

		logger.error("[TimestampConverter.convertTimestampTz] could not convert value\"{}\", aborting conversion with error \"{}\"",
			     val, e.getMessage());

	    }

	    return result;

	}

	public String convertTimeTz(String val) {

	    String result = val;

            /*
             * Only convert it into a different Timezone if requested, otherwise
             * direct 'val' to convertTime() to do the conversion without timezone.
             */
            if (convert_timetz) {
                try {

                    /*
                     * Currently we expect the raw value in UTC.
                     */
                    LocalTime lt = LocalTime.parse(val, pgTimeInputFormatter);
                    ZoneOffset utc = ZoneOffset.of("+0000");
                    ZoneId zid = ZoneId.of(tzstring);

                    if (debug)
                        logger.info("[TimestampConverter.convertTimeTz] using conversion time zone \"{}\"", zid.getId());

                    OffsetTime zt = lt.atOffset(utc);
                    OffsetTime zt_tz = zt.withOffsetSameInstant(LocalDateTime.now().atZone(zid).getOffset());
                    DateTimeFormatter result_formatter = DateTimeFormatter.ofPattern(strTimeFormat);
                    result = result_formatter.format(zt_tz);

                } catch (DateTimeParseException e) {

                    logger.error("[TimestampConverter.convertTz] could not convert value\"{}\", aborting conversion with error \"{}\"",
                                 val, e.getMessage());

                }

	    } else {

                result = convertTime(val);

            }

	    return result;

	}

	public String run(String raw_value, boolean isTime) {

	    String result = raw_value;

	    switch(column.typeName()) {
	    case "timetz":
		result = convertTimeTz(raw_value);
		break;
	    case "time":
		result = convertTime(raw_value);
		break;
	    case "date":
		result = convertDate(raw_value, isTime);
		break;
	    case "timestamptz":
		result = convertTimestampTz(raw_value);
		break;
	    case "timestamp":
		result = convertTimestamp(raw_value);
		break;
	    default:
		/* fall back */
	    }

	    return result;
	}

    }

    /* Input DateTimeFormatter objects for PostgreSQL Converter. */
    private DateTimeFormatter pgTimestampInputFormatter = null;
    private DateTimeFormatter pgTimeInputFormatter      = null;
    private DateTimeFormatter pgDateInputFormatter      = null;

    private boolean useDefaultDatetimeInput = false;
    private boolean useDefaultTimeInput = false;
    private boolean useDefaultDateInput = false;

    public String strDatetimeFormat, strDateFormat, strTimeFormat,
	strDefaultDatetimeInputFormat, strDefaultTimeInputFormat, strDefaultDateInputFormat;

    /*
     * The following parameters control the input format
     * we want to derive
     */
    public static final String DEFAULT_DATETIME_INPUT_FORMAT = "yyyy-MM-dd'T'HH:mm[:ss]";
    public static final String DEFAULT_TIME_INPUT_FORMAT     = "HH:mm[:ss]";
    public static final String DEFAULT_DATE_INPUT_FORMAT     = "yyyy-MM-dd";

    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";
    public static final String DEFAULT_TIME_ZONE   = "UTC";

    public static final List<String> SUPPORTED_DATA_TYPES = List.of("date", "time", "datetime", "timestamp",
								    "timetz", "timestamptz", "datetime2");

    public boolean debug;
    public InternalConverter myconverter = null;
    public String tzstring;
    public boolean convert_timetz;

    private SchemaBuilder datetimeSchema = SchemaBuilder.string().optional().name("oryanmoshe.time.DateTimeString");

    /*
     * Specifies the converter to be used internally. Default is
     * InternalConverter.CONVERT_MODE_GENERIC
     */
    private int convertMode = InternalConverter.CONVERT_MODE_GENERIC;

    /**
     * Logger facility object. Used to print messages to the debezium log.
     */
    private Logger logger = LoggerFactory.getLogger(TimestampConverter.class);

    /**
     * Internal DateTimeFormatter characteristics.
     */
    private enum ConvertFormatterType {
        TIME_INPUT_FORMATTER, TIMESTAMP_INPUT_FORMATTER, DATE_INPUT_FORMATTER
    }

    /**
     * Prints, depending on the converter used, all available
     * time zones or offsets.
     */
    private void logAvailableTimeZones() {

	if (myconverter != null) {
	    myconverter.printAvailableTimeZones();
	}

    }

    /**
     * Returns a DateTimeFormatter suitable for internal use.
     *
     * Pattern must be not null, otherwise null is returned.
     */
    private DateTimeFormatter inputFormatter(String pattern, ConvertFormatterType type, boolean defaultPattern) {

        DateTimeFormatter result = null;

        if (pattern == null)
            return result;

        switch (type) {
        case TIME_INPUT_FORMATTER:
            {
                if (defaultPattern) {
                    result = new DateTimeFormatterBuilder().appendPattern(strDefaultTimeInputFormat)
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).appendOptional(DateTimeFormatter.ofPattern("'Z'")).toFormatter();
                } else {
                    result = DateTimeFormatter.ofPattern(strDefaultTimeInputFormat);
                }
            }
            break;

        case TIMESTAMP_INPUT_FORMATTER:
            {
                if (defaultPattern) {
                    result = new DateTimeFormatterBuilder().appendPattern(strDefaultDatetimeInputFormat)
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).appendOptional(DateTimeFormatter.ofPattern("'Z'")).toFormatter();
                } else {
                    result = DateTimeFormatter.ofPattern(strDefaultDatetimeInputFormat);
                }
            }
            break;

        case DATE_INPUT_FORMATTER:
            result = DateTimeFormatter.ofPattern(strDefaultDateInputFormat);
            break;
        }

        return result;
    }

    @Override
    public void configure(Properties props) {

        debug = props.getProperty("debug", "false").equals("true");

	this.strDatetimeFormat = props.getProperty("format.datetime", DEFAULT_DATETIME_FORMAT);
        this.strDateFormat = props.getProperty("format.date", DEFAULT_DATE_FORMAT);
        this.strTimeFormat = props.getProperty("format.time", DEFAULT_TIME_FORMAT);

	this.tzstring = props.getProperty("format.timezone", DEFAULT_TIME_ZONE);
	logger.info("[TimestampConverter] conversion timezone is {}", this.tzstring);

        if (debug)
	    logger.info("[TimestampConverter.configure] Finished configuring formats. strDatetimeFormat: {}, "
			+ "strTimeFormat: {} strDateFormat: {}, timezone: {}",
			this.strDatetimeFormat, this.strTimeFormat, this.strDateFormat, this.tzstring);

	/* Set convert mode, if specified */
	if (props.getProperty("convert.mode", "generic").equals("postgresql")) {

            /*
             * The following input format settings are optional for the postgresql converter.
             */
            this.strDefaultDatetimeInputFormat = props.getProperty("convert.postgresql.format.datetime", DEFAULT_DATETIME_INPUT_FORMAT);

            if (strDefaultDatetimeInputFormat == DEFAULT_DATETIME_INPUT_FORMAT) {
                this.useDefaultDatetimeInput = true;
            }

            this.strDefaultTimeInputFormat     = props.getProperty("convert.postgresql.format.time", DEFAULT_TIME_INPUT_FORMAT);

            if (strDefaultTimeInputFormat == DEFAULT_TIME_INPUT_FORMAT) {
                this.useDefaultTimeInput = true;
            }

            this.strDefaultDateInputFormat     = props.getProperty("convert.postgresql.format.date", DEFAULT_DATE_INPUT_FORMAT);

            if (strDefaultDateInputFormat == DEFAULT_DATE_INPUT_FORMAT) {
                this.useDefaultDateInput = true;
            }

            this.convert_timetz
                = (props.getProperty("convert.postgresql.timetz.timezone", "false").equals("true")) ? true : false;

	    this.convertMode = InternalConverter.CONVERT_MODE_POSTGRESQL;

            this.pgTimestampInputFormatter = inputFormatter(this.strDefaultDatetimeInputFormat,
                                                            ConvertFormatterType.TIMESTAMP_INPUT_FORMATTER,
                                                            this.useDefaultDatetimeInput);
            this.pgTimeInputFormatter      = inputFormatter(this.strDefaultTimeInputFormat,
                                                            ConvertFormatterType.TIME_INPUT_FORMATTER,
                                                            this.useDefaultTimeInput);
            this.pgDateInputFormatter      = inputFormatter(this.strDefaultDateInputFormat,
                                                            ConvertFormatterType.DATE_INPUT_FORMATTER,
                                                            this.useDefaultDateInput);

	    if (debug)
		logger.info("[TimestampConverter.configure] using postgresql specific datatype conversion");

	}

	/* initialize internal converter, default is "generic" */
	this.myconverter = getConverter(this.convertMode);

	if (debug) {
	    logger.info("[TimestampConverter] conversion rules instantiated, printing available time zones for verification");
	    logAvailableTimeZones();
	}

    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {

        if (debug)
	    logger.info("[TimestampConverter.converterFor] Starting to register column. column.name: {}, column.typeName: {}",
			column.name(), column.typeName());
        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(column.typeName()))) {
            boolean isTime = "time".equalsIgnoreCase(column.typeName());
            registration.register(datetimeSchema, rawValue -> {

		    String convertedValue = "";

		    if (rawValue == null)
			return rawValue;

		    if (debug)
			logger.info("[TimestampConverter.converterFor] Raw Value: {}",
				    rawValue);

		    myconverter.column = column;

		    if (myconverter == null) {
			logger.error("[TimestampConverter.converterFor] could not create suitable converter, returning raw value {}",
				     rawValue.toString());
			return rawValue.toString();
		    }

		    convertedValue =  myconverter.run(rawValue.toString(), isTime);

		    if (debug)
			logger.info("[TimestampConverter.converterFor] After conversion. column.name: {}, column.typeName: {}, convertedValue: {}",
				    column.name(), column.typeName(), convertedValue);

		    return convertedValue;

		});
	}
    }

}
