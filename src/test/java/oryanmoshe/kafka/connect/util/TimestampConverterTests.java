package oryanmoshe.kafka.connect.util;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.spi.converter.RelationalColumn;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

import java.util.OptionalInt;
import java.util.Properties;
import java.util.TimeZone;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimestampConverterTests {
    private class MockRegistration<S> implements ConverterRegistration<S> {
        public Converter _converter;
        public S _schema;

        @Override
        public void register(S fieldSchema, Converter converter) {
            _converter = converter;
            _schema = fieldSchema;
        }
    }

    @ParameterizedTest
    @CsvSource({ "YY-MM-dd, YY-MM-dd", "," + TimestampConverter.DEFAULT_DATE_FORMAT })
    void configureDateTest(final String inputFormat, final String expectedFormat) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (inputFormat != null)
            props.put("format.date", inputFormat);

        final String beforeConfig = tsConverter.strDateFormat;
        assertEquals(null, beforeConfig, beforeConfig + " before configuration, should equal " + null);
        System.out.println(beforeConfig);

        tsConverter.configure(props);

        final String actualResult = tsConverter.strDateFormat;
        assertEquals(expectedFormat, actualResult,
                actualResult + " after configuration, should equal " + expectedFormat);
        System.out.println(actualResult);
    }

    @ParameterizedTest
    @CsvSource({ "mm:ss.SSS, mm:ss.SSS", "," + TimestampConverter.DEFAULT_TIME_FORMAT })
    void configureTimeTest(final String inputFormat, final String expectedFormat) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (inputFormat != null)
            props.put("format.time", inputFormat);

        final String beforeConfig = tsConverter.strTimeFormat;
        assertEquals(null, beforeConfig, beforeConfig + " before configuration, should equal " + null);
        System.out.println(beforeConfig);

        tsConverter.configure(props);

        final String actualResult = tsConverter.strTimeFormat;
        assertEquals(expectedFormat, actualResult,
                actualResult + " after configuration, should equal " + expectedFormat);
        System.out.println(actualResult);
    }

    @ParameterizedTest
    @CsvSource({"01:15:45Z, 03:15:45 +02, Africa/Harare, HH:mm:ss X",
                "23:59:01Z, 01:59:01 +0200, Africa/Harare, HH:mm:ss Z"})
    void converterTestPGTimeTzWithConversion(String input, String expected, String tzstring, String outputFormat) {

        final TimestampConverter tsConverter = new TimestampConverter();

        String columnType = "timetz";
        RelationalColumn mockColumn = getMockColumn(columnType);
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();
        Properties props = new Properties();

        props.put("convert.mode", "postgresql");
        props.put("format.timezone", tzstring);
        props.put("convert.postgresql.timetz.timezone", "true");

        if (outputFormat != null)
            props.put("format.time", outputFormat);

        tsConverter.configure(props);

        String format = tsConverter.strTimeFormat;
        tsConverter.converterFor(mockColumn, mockRegistration);
        System.out.println(mockRegistration._schema.name());

        String actualResult = mockRegistration._converter.convert(input).toString();
        System.out.println(actualResult);
        assertEquals(expected, actualResult,
                     String.format(
                                   "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                                   columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));
    }

    @ParameterizedTest
    @CsvSource({"2020, 2020"})
    void converterTestPGTimestampTzUTCYear(int start_year, int end_year) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        props.put("convert.mode", "postgresql");
        props.put("convert.timezone", "UTC");
        props.put("format.datetime", "dd.MM.yyyy HH:mm:ss.SS ZZZ");

        tsConverter.configure(props);

        String format = tsConverter.strDatetimeFormat;
        String columnType = "timestamptz";

        int[] days_of_month = new int[12];
        days_of_month[0] = 31;
        days_of_month[1] = 29;
        days_of_month[2] = 31;
        days_of_month[3] = 30;
        days_of_month[4] = 31;
        days_of_month[5] = 30;
        days_of_month[6] = 31;
        days_of_month[7] = 31;
        days_of_month[8] = 30;
        days_of_month[9] = 31;
        days_of_month[10] = 30;
        days_of_month[11] = 31;

        for (int year = start_year; year <= end_year; year++) {
            for (int i = 0; i < days_of_month.length; i++) {

                for (int days = 1; days <= days_of_month[i]; days++) {

                    String input = year + "-" + String.format("%02d", (i + 1)) + "-"
                        + String.format("%02d", days) + "T02:15:15.1234";
                    String expectedResult = String.format("%02d", days) + "."
                        + String.format("%02d", (i + 1)) + "." + year + " 02:15:15.12 +0000";

                    RelationalColumn mockColumn = getMockColumn(columnType);
                    MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

                    tsConverter.converterFor(mockColumn, mockRegistration);

                    System.out.println(mockRegistration._schema.name());

                    String actualResult = mockRegistration._converter.convert(input).toString();
                    System.out.println(actualResult);
                    assertEquals(expectedResult, actualResult,
                                 String.format(
                                               "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                                               columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));

                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"2020, 2020"})
    void converterTestPGTimestampUTCYear(int start_year, int end_year) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        props.put("convert.mode", "postgresql");
        props.put("convert.timezone", "UTC");
        props.put("format.datetime", "dd.MM.yyyy HH:mm:ss.SS");

        tsConverter.configure(props);

        String format = tsConverter.strDatetimeFormat;
        String columnType = "timestamp";

        int[] days_of_month = new int[12];
        days_of_month[0] = 31;
        days_of_month[1] = 29;
        days_of_month[2] = 31;
        days_of_month[3] = 30;
        days_of_month[4] = 31;
        days_of_month[5] = 30;
        days_of_month[6] = 31;
        days_of_month[7] = 31;
        days_of_month[8] = 30;
        days_of_month[9] = 31;
        days_of_month[10] = 30;
        days_of_month[11] = 31;

        for (int year = start_year; year <= end_year; year++) {
            for (int i = 0; i < days_of_month.length; i++) {

                for (int days = 1; days <= days_of_month[i]; days++) {

                    /*
                     * Debezium passes the timestamp without time zone usually like timestamptz
                     * always with UTC time zone offset ('Z'), but we omit them here to test
                     * the pattern matching compatibility for "optional" UTC specification here.
                     */
                    String input = year + "-" + String.format("%02d", (i + 1)) + "-"
                        + String.format("%02d", days) + "T02:15:15.1234";
                    String expectedResult = String.format("%02d", days) + "."
                        + String.format("%02d", (i + 1)) + "." + year + " 02:15:15.12";

                    RelationalColumn mockColumn = getMockColumn(columnType);
                    MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

                    tsConverter.converterFor(mockColumn, mockRegistration);

                    System.out.println(mockRegistration._schema.name());

                    String actualResult = mockRegistration._converter.convert(input).toString();
                    System.out.println(actualResult);
                    assertEquals(expectedResult, actualResult,
                                 String.format(
                                               "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                                               columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));

                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"dd-MM-yyyy'T'HH:mm:ss X, 01-01-2020T14:45:15 +01, 2020-01-01T03:45:15.000, US/Samoa, timestamptz",
                "HH:mm:ss X, 14:45:15 +01, 03:45:15.000, US/Samoa, timetz"})
    void inputTestPGTZ(final String inputFormat, final String input, final String expected,
                       final String tzstring, final String columnType) {

        final TimestampConverter tsConverter = new TimestampConverter();
        Properties props = new Properties();

        props.put("convert.mode", "postgresql");
        props.put("format.timezone", tzstring);

        if (columnType.equals("timestamptz")) {
            props.put("convert.postgresql.format.datetime", inputFormat);
        } else if (columnType.equals("timetz")) {
            props.put("convert.postgresql.format.time", inputFormat);
            props.put("convert.postgresql.timetz.timezone", "true");
        } else {
            assertEquals("supported types (timestamptz, timetz)", columnType,
                         "test supports type timestamptz, timetz, got " + columnType);
        }

        props.put("format.datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS");
        props.put("format.time", "HH:mm:ss.SSS");
        tsConverter.configure(props);

        RelationalColumn mockColumn = getMockColumn(columnType);
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        System.out.println(mockRegistration._schema.name());

        String actualResult = mockRegistration._converter.convert(input).toString();
        System.out.println(actualResult);
        assertEquals(expected, actualResult,
                     String.format(
                                   "columnType: timestamptz, format: %s, input: %s, actualDatetimeInputFormat: %s, actualTimeInputFormat: %s, props: %s",
                                   inputFormat, input, tsConverter.strDefaultDatetimeInputFormat,
                                   tsConverter.strDefaultTimeInputFormat, props));

    }

    @ParameterizedTest
    @CsvSource({"dd.MM.yyyy 'T' HH:mm:ss ZZZZZ, 2020-06-01T18:15:45.12345Z, 02.06.2020 T 02:15:45 +08:00, Singapore, format.datetimetz",
                "MM/dd/yyyy HH:mm:ss.S, 2020-03-29T01:15:00Z, 03/29/2020 03:15:00.0, CET, format.datetimetz",
                "MM/dd/yyyy HH:mm:ss.S, 2020-03-29T01:15:00Z, 03/29/2020 03:15:00.0, CET, format.datetime",
                "YY-MM-dd HH:mm X, 2020-01-01T14:45:59.123456789Z, 20-01-01 03:45 -11, US/Samoa, format.datetimetz",
                "YY-MM-dd HH:mm X, 2020-01-01T14:45:59.123456789Z, 20-01-01 15:45 +01, CET, format.datetimetz"})
    void convertTestPGTZFormatStrings(final String format, final String input,
                                      final String expected, final String tzstring, final String format_setting) {

        final TimestampConverter tsConverter = new TimestampConverter();
        Properties props = new Properties();

        props.put("convert.mode", "postgresql");
        props.put(format_setting, format);
        props.put("format.timezone", tzstring);

        tsConverter.configure(props);

        RelationalColumn mockColumn = getMockColumn("timestamptz");
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        System.out.println(mockRegistration._schema.name());

        String actualResult = mockRegistration._converter.convert(input).toString();
        System.out.println(actualResult);
        assertEquals(expected, actualResult,
                     String.format(
                                   "columnType: timestamptz, format: %s, input: %s, actualDatetimeFormat: %s, actualDatetimeTzFormat: %s, props: %s",
                                   format, input, tsConverter.strDatetimeFormat, tsConverter.strDatetimeTzFormat, props));

    }

    @ParameterizedTest
    @CsvSource({ "date, yyyy-MM-dd, 2020-04-16, 2020-04-16", "date,, 2020-04-16, 2020-04-16", "time, mm:ss.SSS, 00:00:02.230, 00:02.230",
                "time,, 00:00:02.230, 00:00:02.230", "timestamp, yyyy-MM-dd, 2020-04-16T00:02, 2020-04-16",
                "timetz,,00:00:02.230Z, 00:00:02.230",
                "timestamptz, dd.MM.yyyy HH:mm:ss, 2020-03-29T01:15, 29.03.2020 03:15:00",
                "timestamp, MM/dd/yyyy HH:mm:ss.SSSSSS, 2020-03-29T03:15:45.123456, 03/29/2020 03:15:45.123456",
                "timestamptz, yyyy-MM-dd HH:mm:ss, 2020-03-29T01:15:45.111110Z, 2020-03-29 03:15:45"})
    void converterTestPGTZ(final String columnType, final String format, final String input, final String expectedResult) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (format != null) {
            props.put(
                    String.format("format.%s",
                            columnType.equals("timestamp") || columnType.equals("timestamptz")
                                  ? "datetime"
                                  : ((columnType.equals("timetz")) ? "time" : columnType)),
                    format);
        }

        props.put("convert.mode", "postgresql");
        props.put("format.timezone", "Europe/Berlin");
        tsConverter.configure(props);

        RelationalColumn mockColumn = getMockColumn(columnType);
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        System.out.println(mockRegistration._schema.name());

        String actualResult = mockRegistration._converter.convert(input).toString();
        System.out.println(actualResult);
        assertEquals(expectedResult, actualResult,
                String.format(
                        "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                        columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));
    }

    @ParameterizedTest
    @CsvSource({"dd.MM.yyyy HH:mm:ss.SSSSSS, dd.MM.yyyy HH:mm:ss.SSSSSS", "," + TimestampConverter.DEFAULT_DATETIME_INPUT_FORMAT})
    void configurePGInputDateTimeFormat(final String inputFormat, final String expectedFormat) {

        final TimestampConverter tsConverter = new TimestampConverter();
        Properties props = new Properties();
        final String beforeConfig = tsConverter.strDefaultDatetimeInputFormat;

        System.out.println("input datetime format: " + inputFormat);

        if (inputFormat != null) {
            props.put("convert.postgresql.format.datetime", inputFormat);
        }

        /* Force "postgresql" conversion mode, otherwise these is a no-op... */
        props.put("convert.mode", "postgresql");

        assertEquals(null, beforeConfig, beforeConfig + " before configuration should equal " + null);

        tsConverter.configure(props);

        final String actualResult = tsConverter.strDefaultDatetimeInputFormat;

        assertEquals(expectedFormat, actualResult,
                     actualResult + " after configuration, should equal " + expectedFormat);
        System.out.println(actualResult);

    }

    @ParameterizedTest
    @CsvSource({ "Europe/Berlin, Europe/Berlin", "," + TimestampConverter.DEFAULT_TIME_ZONE })
    void configureDateTimeZoneTest(final String inputFormat, final String expectedFormat) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (inputFormat != null)
            props.put("format.timezone", inputFormat);

	final String beforeConfig = tsConverter.tzstring;
        assertEquals(null, beforeConfig, beforeConfig + " before configuration, should equal " + null);
        System.out.println("null");

        tsConverter.configure(props);

	final String actualResult = tsConverter.myconverter.getTimeZoneID();

        assertEquals(expectedFormat, actualResult,
		     actualResult + " after configuration, should equal " + expectedFormat);
        System.out.println(actualResult);
    }

    @ParameterizedTest
    @CsvSource({ "date, yyyy-MM-dd, 18368, 2020-04-16", "date,, 18368, 2020-04-16", "time, mm:ss.SSS, 2230, 00:02.230",
            "time,, 2230, 00:00:02.230", "datetime, yyyy-MM-dd, 1587042000279, 2020-04-16",
            "datetime,, 1587042000279, 2020-04-16T13:00:00.279Z", "timestamp, yyyy-MM-dd, 1587042000279, 2020-04-16",
            "datetime2,, 1587042000279, 2020-04-16T13:00:00.279Z", "datetime2, yyyy-MM-dd, 1587042000279, 2020-04-16",
            "timestamp,, 1587042000279, 2020-04-16T13:00:00.279Z", "date, yyyy-MM-dd, 2019-04-19, 2019-04-19",
            "datetime,, 2019-04-19 15:13:20.345123, 2019-04-19T15:13:20.345Z", "time,, 15:13:20, 15:13:20.000",
            "time,HH:mm:ss, 15:13:20, 15:13:20", "timestamp,, 2019-04-19 15:13:20, 2019-04-19T15:13:20.000Z",
            "datetime,, 19-Apr-2019 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime,, 19/04/2019 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime,, 2019-4-19 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime2,, 2019-4-19 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime,, 2019-4-19 3:1:0.345123, 2019-04-19T03:01:00.345Z" })
    void converterTest(final String columnType, final String format, final String input, final String expectedResult) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (format != null)
            props.put(
                    String.format("format.%s",
                            columnType.equals("timestamp") || columnType.equals("datetime2") ? "datetime" : columnType),
                    format);
        tsConverter.configure(props);

        RelationalColumn mockColumn = getMockColumn(columnType);
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        System.out.println(mockRegistration._schema.name());

        String actualResult = mockRegistration._converter.convert(input).toString();
        System.out.println(actualResult);
        assertEquals(expectedResult, actualResult,
                String.format(
                        "columnType: %s, format: %s, input: %s, actualTimeFormat: %s, actualDateFormat: %s, props: %s",
                        columnType, format, input, tsConverter.strTimeFormat, tsConverter.strDateFormat, props));
    }

    RelationalColumn getMockColumn(String type) {
        switch (type) {
        case "date":
            return getDateColumn();
        case "time":
            return getTimeColumn();
        case "timetz":
            return getTimeTzColumn();
        case "datetime":
            return getDateTimeColumn();
        case "datetime2":
            return getDateTime2Column();
        case "timestamp":
            return getTimestampColumn();
        case "timestamptz":
            return getTimestampTzColumn();
        default:
            return null;
        }
    }

    RelationalColumn getDateColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "date";
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getTimeColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "time";
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getTimeTzColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "timetz";
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getDateTimeColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "datetime";
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getDateTime2Column() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "datetime2";
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getTimestampColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "timestamp";
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }

    RelationalColumn getTimestampTzColumn() {
        return new RelationalColumn() {

            @Override
            public String typeName() {
                return "timestamptz";
            }

            @Override
            public String name() {
                return null;
            }

            @Override
            public String dataCollection() {
                return null;
            }

            @Override
            public String typeExpression() {
                return null;
            }

            @Override
            public OptionalInt scale() {
                return null;
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return null;
            }

            @Override
            public int jdbcType() {
                return 0;
            }

            @Override
            public boolean isOptional() {
                return false;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public Object defaultValue() {
                return null;
            }
        };
    }
}
