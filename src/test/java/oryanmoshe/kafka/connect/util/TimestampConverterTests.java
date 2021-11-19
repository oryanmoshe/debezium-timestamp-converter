package oryanmoshe.kafka.connect.util;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.spi.converter.RelationalColumn;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimestampConverterTests {
    private static class MockRegistration<S> implements ConverterRegistration<S> {
        public Converter _converter;
        public S _schema;

        @Override
        public void register(S fieldSchema, Converter converter) {
            _converter = converter;
            _schema = fieldSchema;
        }
    }

    @Test
    void converterDateTimeStampTest() {
        final String columnType = "timestamp";
        final String format = "yyyy-MM-dd HH:mm:ss.SSSSSS";
        final String input = "1637208584795307";

        final String expectedResult = "2021-11-18 04:09:44.795307";

        final Properties props = new Properties();
        props.putAll(Map.of(String.format("format.%s", "datetime"),format, "debug", "true"));

        final TimestampConverter tsConverter = new TimestampConverter();
        tsConverter.configure(props);

        final RelationalColumn mockColumn = getMockColumn(columnType);
        final MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        final Object actualResult = mockRegistration._converter.convert(input);

        assertEquals(expectedResult, actualResult);


        // Convert reverse
        final String columnType2 = "datetime";
        final RelationalColumn mockColumn2 = getMockColumn(columnType2);
        tsConverter.converterFor(mockColumn2, mockRegistration);

        final Object actualResult2 = mockRegistration._converter.convert(expectedResult);

        assertEquals(actualResult2, expectedResult);
    }

    @Test
    void converterDate() {
        final String columnType = "date";
        final String format = "YYYY-MM-dd";
        final String input = "18368";
        final String expectedResult = "2020-04-16";

        final Properties props = new Properties();
        props.putAll(Map.of(String.format("format.%s", "datetime"),format, "debug", "true"));

        final TimestampConverter tsConverter = new TimestampConverter();
        tsConverter.configure(props);

        final RelationalColumn mockColumn = getMockColumn(columnType);
        final MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        final Object actualResult = mockRegistration._converter.convert(input);

        assertEquals(expectedResult, actualResult);


        // Convert reverse
        final String columnType2 = "datetime";
        final RelationalColumn mockColumn2 = getMockColumn(columnType2);
        tsConverter.converterFor(mockColumn2, mockRegistration);

        final Object actualResult2 = mockRegistration._converter.convert(expectedResult);

        assertEquals(actualResult2, expectedResult);
    }

    @Test
    void converterDate2() {
        final String columnType = "datetime";
        final String format = "YYYY-MM-dd";
        final String input = "1587042000279";
        final String expectedResult = "2020-04-16";

        final Properties props = new Properties();
        props.putAll(Map.of(String.format("format.%s", "datetime"),format, "debug", "true"));

        final TimestampConverter tsConverter = new TimestampConverter();
        tsConverter.configure(props);

        final RelationalColumn mockColumn = getMockColumn(columnType);
        final MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        final Object actualResult = mockRegistration._converter.convert(input);

        assertEquals(expectedResult, actualResult);


        // Convert reverse
        final String columnType2 = "datetime";
        final RelationalColumn mockColumn2 = getMockColumn(columnType2);
        tsConverter.converterFor(mockColumn2, mockRegistration);

        final Object actualResult2 = mockRegistration._converter.convert(expectedResult);

        assertEquals(actualResult2, expectedResult);
    }

    @Test
    void converterTime() {
        final String columnType = "time";
        final String format = "HH:mm:ss.SSS";
        final String input = "2230";
        final String expectedResult = "00:00:02.230";

        final Properties props = new Properties();
        props.putAll(Map.of(String.format("format.%s", columnType), format, "debug", "true"));

        final TimestampConverter tsConverter = new TimestampConverter();
        tsConverter.configure(props);

        final RelationalColumn mockColumn = getMockColumn(columnType);
        final MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        final Object actualResult = mockRegistration._converter.convert(input);

        assertEquals(expectedResult, actualResult);

        // Convert reverse
        final RelationalColumn mockColumn2 = getMockColumn(columnType);
        tsConverter.converterFor(mockColumn2, mockRegistration);

        final Object actualResult2 = mockRegistration._converter.convert(expectedResult);

        assertEquals(actualResult2, expectedResult);
    }

    @Test
    void converterTime2() {
        final String columnType = "time";
        final String input = "15:13:20";
        final String expectedResult = "15:13:20.000";

        final Properties props = new Properties();
        props.putAll(Map.of("debug", "true"));

        final TimestampConverter tsConverter = new TimestampConverter();
        tsConverter.configure(props);

        final RelationalColumn mockColumn = getMockColumn(columnType);
        final MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        final Object actualResult = mockRegistration._converter.convert(input);

        assertEquals(expectedResult, actualResult);

        // Convert reverse
        final RelationalColumn mockColumn2 = getMockColumn(columnType);
        tsConverter.converterFor(mockColumn2, mockRegistration);

        final Object actualResult2 = mockRegistration._converter.convert(expectedResult);

        assertEquals(actualResult2, expectedResult);
    }

    @ParameterizedTest
    @CsvSource({ "date, YYYY-MM-dd, 18368, 2020-04-16",
            "date,, 18368, 2020-04-16",
            "time, mm:ss.SSS, 2230, 00:02.230",
            "time,, 2230, 00:00:02.230",
            "datetime, YYYY-MM-dd, 1587042000279, 2020-04-16",
            "datetime,, 1587042000279, 2020-04-16T13:00:00.279Z",
            "timestamp, YYYY-MM-dd, 1587042000279, 2020-04-16",
            "datetime2,, 1587042000279, 2020-04-16T13:00:00.279Z",
            "datetime2, YYYY-MM-dd, 1587042000279, 2020-04-16",
            "timestamp,, 1587042000279, 2020-04-16T13:00:00.279Z",
            "date, YYYY-MM-dd, 2019-04-19, 2019-04-19",
            "datetime,, 2019-04-19 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "time,, 15:13:20, 15:13:20.000",
            "time,HH:mm:ss, 15:13:20, 15:13:20",
            "timestamp,, 2019-04-19 15:13:20, 2019-04-19T15:13:20.000Z",
            "datetime,, 19-Apr-2019 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime,, 19/04/2019 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime,, 2019-4-19 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime2,, 2019-4-19 15:13:20.345123, 2019-04-19T15:13:20.345Z",
            "datetime,, 2019-4-19 3:1:0.345123, 2019-04-19T03:01:00.345Z",
            "datetime,YYYY-MM-dd,,",
            "timestamp,,,", "date,,,"})
    void converterTest(final String columnType, final String format, final String input, final String expectedResult) {
        final TimestampConverter tsConverter = new TimestampConverter();

        Properties props = new Properties();
        if (format != null)
            props.put(
                    String.format("format.%s",
                            columnType.equals("timestamp") || columnType.equals("datetime2") ? "datetime" : columnType),
                    format);
        props.put("debug", "true");
        tsConverter.configure(props);

        RelationalColumn mockColumn = getMockColumn(columnType);
        MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<>();

        tsConverter.converterFor(mockColumn, mockRegistration);

        Object actualResult = mockRegistration._converter.convert(input);
        if (actualResult != null) { assertEquals(expectedResult, actualResult); }
    }

    RelationalColumn getMockColumn(String type) {
        switch (type) {
            case "date":
                return getDateColumn();
            case "time":
                return getTimeColumn();
            case "datetime":
                return getDateTimeColumn();
            case "datetime2":
                return getDateTime2Column();
            case "timestamp":
                return getTimestampColumn();
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
                return "datecolumn";
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
                return OptionalInt.empty();
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return OptionalInt.empty();
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
                return "timecolumn";
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
                return OptionalInt.empty();
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return OptionalInt.empty();
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
                return "datetimecolumn";
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
                return OptionalInt.empty();
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return OptionalInt.empty();
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
                return true;
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
                return "datetime2column";
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
                return OptionalInt.empty();
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return OptionalInt.empty();
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
                return "timestampcolumn";
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
                return OptionalInt.empty();
            }

            @Override
            public int nativeType() {
                return 0;
            }

            @Override
            public OptionalInt length() {
                return OptionalInt.empty();
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