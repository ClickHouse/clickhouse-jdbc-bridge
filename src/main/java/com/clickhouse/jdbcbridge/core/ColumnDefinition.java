/**
 * Copyright 2019-2022, Zhichun Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.clickhouse.jdbcbridge.core;

import static com.clickhouse.jdbcbridge.core.DataType.*;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.Map.Entry;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * This class defines a column.
 * 
 * @since 2.0
 */
public class ColumnDefinition {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ColumnDefinition.class);

    static final boolean DEFAULT_VALUE_SUPPORT = String.valueOf(true)
            .equalsIgnoreCase(Utils.getConfiguration("false", "DEFAULT_VALUE", "jdbc-bridge.type.default"));

    public static final String DEFAULT_NAME = "unknown";
    public static final DataType DEFAULT_TYPE = DataType.Str;

    private static final String CONF_NAME = "name";
    private static final String CONF_TYPE = "type";
    private static final String CONF_VALUE = "value";
    private static final String CONF_OPTIONS = "options";
    private static final String CONF_NULLABLE = "nullable";
    private static final String CONF_LENGTH = "length";
    private static final String CONF_PRECISION = "precision";
    private static final String CONF_SCALE = "scale";
    private static final String CONF_TIMEZONE = "timezone";

    private static final String TOKEN_DEFAULT = " DEFAULT ";
    private static final String NULLABLE_BEGIN = "Nullable(";
    private static final String NULLABLE_END = ")";
    private static final String NULL_VALUE = "null";

    private final String name;
    private final DataType type;
    private final boolean nullable;
    private final int length;
    private final int precision;
    private final int scale;
    private final TimeZone timezone;
    private final boolean hasDefaultValue;

    private final Map<String, Integer> options = new LinkedHashMap<>();

    final TypedParameter<?> value;

    // index in the column list
    private int index = -1;

    static Map<String, Integer> parseOptions(Object opts) {
        Map<String, Integer> options = new LinkedHashMap<>();

        if (opts == null) {
            return options;
        }

        if (opts instanceof JsonObject) {
            JsonObject obj = (JsonObject) opts;
            for (Map.Entry<String, Object> entry : obj) {
                Object v = entry.getValue();
                if (v == null) {
                    continue;
                }
                options.put(entry.getKey(),
                        v instanceof Number ? ((Number) v).intValue() : Integer.parseInt(String.valueOf(v)));
            }
        } else if (opts instanceof JsonArray) {
            JsonArray arr = (JsonArray) opts;
            int index = 0;
            for (Object obj : arr) {
                if (obj == null) {
                    continue;
                }

                final String n;
                final int i;
                if (obj instanceof JsonObject) {
                    JsonObject j = (JsonObject) obj;
                    Object objName = j.getValue(CONF_NAME);
                    Object objValue = j.getValue(CONF_VALUE);
                    if (objName == null || objValue == null) {
                        continue;
                    }
                    n = String.valueOf(objName);
                    i = Integer.parseInt(String.valueOf(objValue));
                } else {
                    n = String.valueOf(obj);
                    i = index++;
                }

                options.put(n, i);
            }
        } else if (opts instanceof Enumeration) {
            Enumeration<?> e = (Enumeration<?>) opts;

            int index = 0;
            while (e.hasMoreElements()) {
                Object optName = e.nextElement();
                if (optName != null) {
                    options.put(String.valueOf(optName), index++);
                }
            }
        } else if (opts instanceof Iterable) {
            int index = 0;
            for (Object o : (Iterable<?>) opts) {
                if (o != null) {
                    options.put(String.valueOf(o), index++);
                }
            }
        } else if (opts.getClass().isArray()) {
            int index = 0;
            for (Object o : (Object[]) opts) {
                if (o != null) {
                    options.put(String.valueOf(o), index++);
                }
            }
        } else if (opts instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) opts).entrySet()) {
                Object optName = entry.getKey();
                Object optValue = entry.getValue();

                if (optName == null || optValue == null) {
                    continue;
                }

                options.put(String.valueOf(optName), Integer.parseInt(String.valueOf(optValue)));
            }
        } else {
            String optsInStr = String.valueOf(opts);

            int index = 0;

            if (optsInStr.indexOf('\'') == -1) {
                for (String str : Utils.splitByChar(optsInStr, ',')) {
                    options.put(str, index++);
                }
            } else {
                StringBuilder sb = new StringBuilder();
                boolean hasQuote = false;
                boolean escaped = false;
                for (int i = 0, len = optsInStr.length(); i < len; i++) {
                    char ch = optsInStr.charAt(i);
                    if (ch == '\\') {
                        escaped = !escaped;
                        continue;
                    }

                    if (escaped) {
                        sb.append(ch);
                        escaped = false;
                        continue;
                    }

                    if (ch == '\'') {
                        hasQuote = !hasQuote;
                    } else if (hasQuote) {
                        sb.append(ch);
                    }

                    if (!hasQuote && sb.length() > 0) {
                        String optName = sb.toString();
                        sb.setLength(0);

                        int startIdx = optsInStr.indexOf('=', i);
                        int endIdx = optsInStr.indexOf(',', i);
                        if (endIdx < 0) {
                            endIdx = len;
                        }

                        i = endIdx;

                        if (endIdx > startIdx) {
                            options.put(optName, startIdx == -1 ? index++
                                    : Integer.parseInt(optsInStr.substring(startIdx + 1, endIdx).trim()));
                        }
                    }
                }
            }
        }

        return options;

    }

    public static ColumnDefinition fromJson(JsonObject json) {
        String name = DEFAULT_NAME;
        DataType type = DataType.Str;
        boolean nullable = DEFAULT_NULLABLE;
        int length = DEFAULT_LENGTH;
        int precision = DEFAULT_PRECISION;
        int scale = DEFAULT_SCALE;
        String timezone = null;
        String value = null;
        Map<String, Integer> options = new LinkedHashMap<>();

        if (json != null) {
            name = json.getString(CONF_NAME, DEFAULT_NAME);
            type = DataType.from(json.getString(CONF_TYPE, DEFAULT_TYPE.name()));
            nullable = json.getBoolean(CONF_NULLABLE, DEFAULT_NULLABLE);
            switch (type) {
                case FixedStr:
                    length = json.getInteger(CONF_LENGTH, DEFAULT_LENGTH);
                    break;
                case DateTime64:
                    scale = json.getInteger(CONF_SCALE, DEFAULT_DATETIME64_SCALE);
                    break;
                case Decimal:
                    precision = json.getInteger(CONF_PRECISION, DEFAULT_PRECISION);
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Decimal256:
                    scale = json.getInteger(CONF_SCALE, DEFAULT_SCALE);
                    break;
                default:
                    break;
            }

            timezone = json.getString(CONF_TIMEZONE);
            value = json.getString(CONF_VALUE);

            options.putAll(parseOptions(json.getValue(CONF_OPTIONS)));
        }

        return new ColumnDefinition(name, type, nullable, length, precision, scale, timezone, value, options);
    }

    public static ColumnDefinition fromObject(Object obj) {
        final ColumnDefinition column;

        if (obj == null) {
            column = new ColumnDefinition(DEFAULT_NAME, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION,
                    DEFAULT_SCALE);
        } else if (obj instanceof ColumnDefinition) {
            column = (ColumnDefinition) obj;
        } else if (obj instanceof JsonObject) {
            column = fromJson((JsonObject) obj);
        } else if (obj instanceof Map) {
            Map<?, ?> m = (Map<?, ?>) obj;
            if (m.size() > 0) {
                Object value = m.get(CONF_NAME);
                String name = value == null ? DEFAULT_NAME : String.valueOf(value);

                value = m.get(CONF_TYPE);
                DataType type = value == null ? DataType.Str : DataType.from(String.valueOf(value));

                value = m.get(CONF_NULLABLE);
                boolean nullable = value == null ? true : Boolean.valueOf(String.valueOf(value));

                value = m.get(CONF_LENGTH);
                int length = value == null ? DEFAULT_LENGTH : Integer.parseInt(String.valueOf(value));

                value = m.get(CONF_PRECISION);
                int precision = value == null ? DEFAULT_PRECISION : Integer.parseInt(String.valueOf(value));

                value = m.get(CONF_SCALE);
                int scale = value == null ? DEFAULT_SCALE : Integer.parseInt(String.valueOf(value));

                value = m.get(CONF_TIMEZONE);
                String timezone = value == null ? null : String.valueOf(value);

                value = m.get(CONF_VALUE);

                column = new ColumnDefinition(name, type, nullable, length, precision, scale, timezone,
                        value == null ? null : String.valueOf(value), parseOptions(m.get(CONF_OPTIONS)));
            } else {
                column = new ColumnDefinition(DEFAULT_NAME, DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION,
                        DEFAULT_SCALE);
            }
        } else

        {
            column = new ColumnDefinition(String.valueOf(obj), DataType.Str, true, DEFAULT_LENGTH, DEFAULT_PRECISION,
                    DEFAULT_SCALE);
        }

        return column;
    }

    // FIXME naive implementation, use ANTLR4 in future release for consistency
    // https://github.com/ClickHouse/ClickHouse/pull/11298
    public static ColumnDefinition fromString(String columnInfo) {
        String name = DEFAULT_NAME;
        DataType type = DEFAULT_TYPE;
        boolean nullable = DEFAULT_NULLABLE;
        int length = DEFAULT_LENGTH;
        int precision = -1;
        int scale = -1;
        String timezone = null;
        String value = null;
        Map<String, Integer> options = new LinkedHashMap<>();

        if (columnInfo != null && (columnInfo = columnInfo.trim()).length() > 0) {
            char quote = columnInfo.charAt(0);
            boolean hasQuote = quote == '`' || quote == '"';
            boolean escaped = false;
            int lastIndex = columnInfo.length() - 1;
            int nameEndIndex = hasQuote ? Math.min(columnInfo.lastIndexOf(quote), lastIndex) : lastIndex;
            StringBuilder sb = new StringBuilder(lastIndex + 1);
            for (int i = hasQuote ? 1 : 0; i <= lastIndex; i++) {
                char ch = columnInfo.charAt(i);
                escaped = !escaped && (ch == '\\'
                        || (hasQuote && ch == quote && columnInfo.charAt(Math.min(i + 1, lastIndex)) == quote));

                if ((hasQuote && !escaped && i == nameEndIndex) || (!hasQuote && Character.isWhitespace(ch))) {
                    name = sb.toString();
                    sb.setLength(0);

                    // type declaration is case-sensitive
                    String declaredType = columnInfo.substring(Math.min(i + 1, lastIndex)).trim();
                    String defaultValue = null;

                    i = columnInfo.length() - 1;

                    // default value. unlike Nullable etc. default is case-insensitive...
                    int defaultIndex = Utils.indexOfKeywordIgnoreCase(declaredType, TOKEN_DEFAULT);
                    if (defaultIndex > 0) {
                        defaultValue = declaredType.substring(defaultIndex + TOKEN_DEFAULT.length()).trim();
                        declaredType = declaredType.substring(0, defaultIndex).trim();

                        if (!defaultValue.isEmpty()) {
                            value = defaultValue.charAt(0) == '\''
                                    && defaultValue.charAt(defaultValue.length() - 1) == '\''
                                            ? defaultValue.substring(1, defaultValue.length() - 1)
                                            : defaultValue;

                            if (NULL_VALUE.equalsIgnoreCase(value)) {
                                value = null;
                            }
                        }
                    }

                    // nullable
                    if (declaredType.startsWith(NULLABLE_BEGIN)) {
                        int suffixIndex = declaredType.lastIndexOf(NULLABLE_END);
                        if (suffixIndex != -1) {
                            nullable = true;
                            declaredType = declaredType.substring(NULLABLE_BEGIN.length(), suffixIndex);
                        } else {
                            log.warn("Discard invalid Nullable declaration [{}]", declaredType);
                        }
                    } else {
                        nullable = false;
                    }

                    // enum*, datetime* and decimal*
                    int index = declaredType.indexOf('(');
                    if (index > 0 && declaredType.charAt(declaredType.length() - 1) == ')') {
                        String innerExpr = declaredType.substring(index + 1, declaredType.length() - 1);
                        List<String> arguments = Utils.splitByChar(innerExpr, ',');
                        type = DataType.from(declaredType.substring(0, index));

                        int size = arguments.size();
                        if (size > 0) {
                            switch (type) {
                                case Enum:
                                case Enum8:
                                case Enum16:
                                    arguments.clear();
                                    options.putAll(parseOptions(innerExpr));
                                    break;
                                case FixedStr:
                                    length = Integer.parseInt(arguments.remove(0));
                                    break;
                                case DateTime64:
                                    scale = Integer.parseInt(arguments.remove(0));
                                case DateTime:
                                    if (arguments.size() > 0) {
                                        String tz = arguments.remove(0).trim();
                                        if (tz.length() > 2 && tz.charAt(0) == '\''
                                                && tz.charAt(tz.length() - 1) == '\'') {
                                            timezone = tz.substring(1, tz.length() - 1);
                                        }
                                    }
                                    break;
                                case Decimal:
                                    precision = Integer.parseInt(arguments.remove(0));
                                case Decimal32:
                                case Decimal64:
                                case Decimal128:
                                case Decimal256:
                                    if (arguments.size() > 0) {
                                        scale = Integer.parseInt(arguments.remove(0));
                                    }
                                    break;
                                default:
                                    log.warn("Discard unsupported arguments for [{}]: {}", declaredType, arguments);
                                    break;
                            }

                            if (arguments.size() > 0) {
                                log.warn("Discard unsupported arguments for [{}]: {}", declaredType, arguments);
                            }
                        } else {
                            log.warn("Discard empty argument for [{}]", declaredType);
                        }
                    } else {
                        type = DataType.from(declaredType);
                    }
                } else if (name == DEFAULT_NAME) {
                    if (!hasQuote || (hasQuote && !escaped)) {
                        sb.append(ch);
                    }
                }
            }

            if (sb.length() > 0) {
                name = sb.toString();
            }
        }

        return new ColumnDefinition(name, type, nullable, length, precision, scale, timezone, value, options);
    }

    public ColumnDefinition(ColumnDefinition def) {
        this.name = Objects.requireNonNull(def).name;
        this.type = def.type;
        this.nullable = def.nullable;
        this.length = def.length;
        this.precision = def.precision;
        this.scale = def.scale;

        this.timezone = def.timezone;
        this.hasDefaultValue = def.hasDefaultValue;
        this.value = new DefaultValues().getTypedValue(type).merge(def.value.getValue());
        this.options.putAll(def.options);
    }

    public ColumnDefinition(String name, DataType type, boolean nullable, int length, int precision, int scale) {
        this(name, type, nullable, length, precision, scale, null, null, null);
    }

    public ColumnDefinition(String name, DataType type, boolean nullable, int length, int precision, int scale,
            String timezone, Object value, Map<String, Integer> options) {
        this.name = name == null ? DEFAULT_NAME : name;
        this.type = type;
        this.nullable = nullable;
        this.timezone = type == DataType.DateTime || type == DataType.DateTime64
                ? (timezone == null ? null : TimeZone.getTimeZone(timezone))
                : null;
        this.hasDefaultValue = DEFAULT_VALUE_SUPPORT && value != null;
        this.value = new DefaultValues().getTypedValue(type).merge(value == null ? null : String.valueOf(value));
        if (options != null) {
            this.options.putAll(options);
        }

        int recommendedPrecision = DEFAULT_PRECISION;
        int recommendedScale = DEFAULT_SCALE;

        switch (type) {
            case DateTime64:
                recommendedPrecision = precision < 0 ? DEFAULT_DATETIME64_PRECISION : precision;
                recommendedScale = scale < 0 ? DEFAULT_DATETIME64_SCALE
                        : (scale > MAX_DATETIME64_SCALE ? MAX_DATETIME64_SCALE : scale);
                break;
            case Decimal:
                recommendedPrecision = DEFAULT_DECIMAL_PRECISON;
                recommendedPrecision = precision <= 0 ? recommendedPrecision
                        : (precision > MAX_PRECISON ? MAX_PRECISON : precision);
                recommendedScale = scale < 0 ? DEFAULT_DECIMAL_SCALE
                        : (scale > recommendedPrecision ? recommendedPrecision : scale);
                break;
            case Decimal32:
                recommendedPrecision = DEFAULT_DECIMAL32_PRECISON;
                recommendedScale = DEFAULT_DECIMAL32_SCALE;
                break;
            case Decimal64:
                recommendedPrecision = DEFAULT_DECIMAL64_PRECISON;
                recommendedScale = DEFAULT_DECIMAL64_SCALE;
                break;
            case Decimal128:
                recommendedPrecision = DEFAULT_DECIMAL128_PRECISON;
                recommendedScale = DEFAULT_DECIMAL128_SCALE;
                break;
            case Decimal256:
                recommendedPrecision = DEFAULT_DECIMAL256_PRECISON;
                recommendedScale = DEFAULT_DECIMAL256_SCALE;
                break;
            default:
                recommendedPrecision = precision < 0 ? DEFAULT_PRECISION : precision;
                break;
        }

        this.length = type == FixedStr ? (length <= 0 ? 1 : length) : type.getLength();
        this.precision = recommendedPrecision < type.getPrecision() ? recommendedPrecision : type.getPrecision();
        this.scale = scale <= 0 ? recommendedScale : (scale > this.precision ? this.precision : scale);
    }

    public String getName() {
        return this.name;
    }

    public DataType getType() {
        return this.type;
    }

    public boolean isNullable() {
        return this.nullable;
    }

    public int getLength() {
        return this.length;
    }

    public int getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }

    public TimeZone getTimeZone() {
        return this.timezone;
    }

    public Object getValue() {
        return this.value.getValue();
    }

    public Map<String, Integer> getOptions() {
        return Collections.unmodifiableMap(this.options);
    }

    public int getOptionValue(String optionName) {
        for (Entry<String, Integer> entry : this.options.entrySet()) {
            // case-insensitive?
            if (entry.getKey().equals(optionName)) {
                return entry.getValue();
            }
        }

        throw new IllegalArgumentException("Unknown option: " + optionName);
    }

    public int requireValidOptionValue(int value) {
        for (int v : this.options.values()) {
            if (v == value) {
                return value;
            }
        }

        throw new IllegalArgumentException("Invalid option: " + value);
    }

    public void setIndex(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Column index is zero-based and should never be negative.");
        }

        if (this.index == -1) {
            this.index = index;
        } else {
            throw new IllegalStateException("Column index can only be set once!");
        }
    }

    public int getIndex() {
        return this.index;
    }

    public boolean isIndexed() {
        return this.index != -1;
    }

    public void writeValueTo(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }

        if (this.isNullable()) {
            buffer.writeNonNull();
        }

        this.value.writeValueTo(buffer, this.getPrecision(), this.getScale(), this.getTimeZone());
    }

    JsonObject toJson() {
        JsonObject col = new JsonObject();
        col.put(CONF_NAME, this.getName());
        col.put(CONF_TYPE, this.getType().name());
        col.put(CONF_NULLABLE, this.isNullable());

        switch (this.getType()) {
            case FixedStr:
                col.put(CONF_LENGTH, this.getLength());
                break;
            case DateTime:
                if (this.getTimeZone() != null) {
                    col.put(CONF_TIMEZONE, this.getTimeZone().getID());
                }
                break;
            case Decimal:
                col.put(CONF_PRECISION, this.getPrecision());
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
            case DateTime64:
                col.put(CONF_SCALE, this.getScale());
                break;
            default:
                break;
        }

        if (this.hasDefaultValue) {
            col.put(CONF_VALUE, this.getValue());
        }

        return col;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        char quote = '`';
        sb.append(quote);
        for (int i = 0; i < this.name.length(); i++) {
            char ch = this.name.charAt(i);
            if (ch == quote) {
                sb.append(quote).append(quote);
            } else {
                sb.append(ch);
            }
        }

        sb.append(quote).append(' ');

        int index = sb.length();

        switch (this.type) {
            case Bool:
                sb.append(DataType.ALIAS_BOOLEAN);
                break;
            case FixedStr:
                sb.append(DataType.ALIAS_FIXED_STRING);
                break;
            case Str:
                sb.append(DataType.ALIAS_STRING);
                break;
            default:
                sb.append(this.type.name());
                break;
        }

        if (this.type == DataType.Enum || this.type == DataType.Enum8 || this.type == DataType.Enum16) {
            sb.append('(');
            boolean isNotFirst = false;
            for (Map.Entry<String, Integer> entry : this.options.entrySet()) {
                if (isNotFirst) {
                    sb.append(',');
                } else {
                    isNotFirst = true;
                }

                sb.append('\'');
                String optName = entry.getKey();
                for (int i = 0, len = optName.length(); i < len; i++) {
                    char ch = optName.charAt(i);
                    if (ch == '\\' || ch == '\'') {
                        sb.append('\\');
                    }
                    sb.append(ch);
                }
                sb.append('\'').append('=').append(entry.getValue());
            }
            sb.append(')');
        } else if (this.type == DataType.Decimal) {
            sb.append('(').append(this.precision).append(',').append(this.scale).append(')');
        } else if (this.type == DataType.Decimal32 || this.type == DataType.Decimal64
                || this.type == DataType.Decimal128 || this.type == DataType.Decimal256) {
            sb.append('(').append(this.scale).append(')');
        } else if (this.type == DataType.DateTime && this.timezone != null) {
            sb.append('(').append('\'').append(this.timezone.getID()).append('\'').append(')');
        } else if (this.type == DataType.DateTime64) {
            sb.append('(').append(this.scale);
            if (this.timezone != null) {
                sb.append(',').append('\'').append(this.timezone.getID()).append('\'');
            }
            sb.append(')');
        } else if (this.type == DataType.FixedStr) {
            sb.append('(').append(this.length).append(')');
        }

        if (this.nullable) {
            sb.insert(index, NULLABLE_BEGIN).append(NULLABLE_END);
        }

        if (this.hasDefaultValue) {
            sb.append(TOKEN_DEFAULT);
            if (this.type == DataType.Str || this.type == DataType.FixedStr || this.type == DataType.UUID
                    || this.type == DataType.IPv6 || this.type == DataType.Enum || this.type == DataType.Enum8
                    || this.type == DataType.Enum16) {
                Object value = this.getValue();
                if (value == null || value instanceof Number) {
                    sb.append(value);
                } else {
                    String str = String.valueOf(value);
                    sb.append('\'');
                    boolean escaped = false;
                    for (int i = 0; i < str.length(); i++) {
                        char ch = str.charAt(i);
                        if (ch == '\\') {
                            escaped = !escaped;
                        } else if (!escaped && ch == '\'') {
                            sb.append('\\');
                        }
                        sb.append(ch);
                    }
                    sb.append('\'');
                }
            } else {
                sb.append(this.getValue());
            }
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (hasDefaultValue ? 1231 : 1237);
        result = prime * result + index;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + (nullable ? 1231 : 1237);
        result = prime * result + length;
        result = prime * result + precision;
        result = prime * result + scale;
        result = prime * result + ((timezone == null) ? 0 : timezone.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;

        ColumnDefinition other = (ColumnDefinition) obj;

        return (index == other.index && nullable == other.nullable && length == other.length
                && precision == other.precision && hasDefaultValue == other.hasDefaultValue && scale == other.scale
                && type == other.type && (name == other.name || (name != null && name.equals(other.name)))
                && (timezone == other.timezone || (timezone != null && timezone.equals(other.timezone)))
                && (value == other.value || (value != null && value.equals(other.value))));
    }
}