![](https://github.com/oryanmoshe/debezium-timestamp-converter/workflows/Run%20Tests/badge.svg?branch=master) ![](https://github.com/oryanmoshe/debezium-timestamp-converter/workflows/GitHub%20Release/badge.svg) ![](https://github.com/oryanmoshe/debezium-timestamp-converter/workflows/GitHub%20Package/badge.svg)
# Debezium Timestamp Converter
This is a custom converter to use with debezium (using their SPI, introduced in version 1.1).
You can use it to convert all temporal data types (in all databases) into a specified format you choose in ```generic``` conversion mode.
There is also a PostgreSQL-specific conversion mode if your data source is PostgreSQL only.
The current ```TimestampConverter``` plugin implementation here is tested with Debezium ```1.3.0 Final```,
but should work with older version until ```1.1``` as well.

## Usage
You can either download the `.jar` file from the [releases](https://github.com/oryanmoshe/debezium-timestamp-converter/releases) and include it in your connector's folder, or add the converter as a dependency to your maven project.

**You have to add this converter to each of your connectors, not just in the main folder (`/kafka/connect`)!**

## Configuration

### Basic Configuration
To configure this converter all you need to do is add the following lines to your connector configuration:
```json
"converters": "timestampConverter",
"timestampConverter.type": "oryanmoshe.kafka.connect.util.TimestampConverter"
```

### Additional Configuration
There are a few configuration settings you can add, here are their default values:
```json
"timestampConverter.format.time": "HH:mm:ss.SSS",
"timestampConverter.format.date": "YYYY-MM-dd",
"timestampConverter.format.datetime": "YYYY-MM-dd'T'HH:mm:ss.SSS'Z'",
"timestampConverter.format.datetimetz": "YYYY-MM-dd'T'HH:mm:ss.SSS'Z'",
"timestampConverter.debug": "false",
"timestampConverter.format.timezone": "UTC",
"timestampConverter.convert.mode": "generic"
```

The parameter ``timestampConverter.format.timezone`` can be used to convert the representation of
timestamp with time zones. In this case, care must be taken if you want to format the converted
``timestamptz`` value to preserve the offset specification. ``timestampConverter.format.datetime``
is used for ``timestamp`` datatype conversion as well, and since this type doesn't support
offset in its pattern representation this would fail if someone converts both datatypes within
the same configuration. To solve this, specify the ```timestampConverter.format.datetimetz``` with the
pattern for ```timestamptz``` values additionally and leave the ```timestampConverter.format.datetime```
for ```timestamp``` alone.

The Timestamp Converter plugin supports two conversion modes: a ```generic``` conversion mode which tries to support
a wide range of temporal input formats, and a ```postgresql``` conversion mode. The latter is dedicated to PostgreSQL
sources and expects a specific input format to avoid any confusion during conversion. To use the PostgreSQL specific
conversion mode, configure the converter as follows:

```json
"timestampConverter.convert.mode": "postgresql"
```

When using the ```postgresql``` conversion mode, additional parameters can be used to configure
its behavior (here with their default parameters):

```json
"timestampConverter.convert.postgresql.format.datetime": "yyyy-MM-dd'T'HH:mm[:ss][.SSS[SSS]]['Z']",
"timestampConverter.convert.postgresql.format.time": "HH:mm[:ss][.SSS[SSS]]['Z']",
"timestampConverter.convert.postgresql.format.date": "yyyy-MM-dd"
```

These parameters controls the format of the input values to use for conversion. Usually their are passed
in ISO format in UTC so that there's no need to adjust them. Note that the syntax of the pattern must follow
the pattern syntax of java.time.DateTimeFormatter, documented here:

https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html

### Experimental Configuration

There is an additional parameter which controls the conversion in ```postgresql``` mode
for its ```timetz``` datatype:

```json
"timestampConverter.convert.postgresql.timetz.timezone": "false"
```

If set to ```true```, the ```TimestampConverter``` tries to convert the raw ```timetz``` value from ```UTC```
to the timezone specified in ```"timestampConverter.format.timezone"```. Please note that this is dangerous: Since
a ```timetz``` is just an arbitrary point in time and debezium delivers the value in ```UTC```, we cannot preserve
the original it was derived from. This is especially a problem for example if timezones have different rules, like
daylight savings. So your're advised to use this with caution.

### Notes

In ```postgresql``` conversion mode, column values of type ```datetime``` and ```datetime2``` aren't converted and returned
as-is.

Theoretically someone might want to have the ```postgresql``` conversion mode available for other
database systems as well. Currently the implementation is tight to the temporal datatypes in
PostgreSQL only. We might want to relax this in future versions.
