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

import static com.clickhouse.jdbcbridge.core.Utils.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

import io.vertx.core.buffer.Buffer;

/**
 * Wrapper class of Vertx {@link io.vertx.core.buffer.Buffer} representing a
 * sequence of zero or more bytes. It provides convinient methods to read /
 * write value of various data types.
 * 
 * @since 2.0
 */
public final class ByteBuffer {
    // self-maintained readerIndex
    protected int position = 0;

    protected final Buffer buffer;
    protected final TimeZone timezone;

    public static ByteBuffer wrap(Buffer buffer, TimeZone timezone) {
        return new ByteBuffer(buffer, timezone);
    }

    public static ByteBuffer wrap(Buffer buffer) {
        return wrap(buffer, TimeZone.getDefault());
    }

    public static ByteBuffer newInstance(int initialSizeHint, TimeZone timezone) {
        return new ByteBuffer(Buffer.buffer(initialSizeHint), timezone);
    }

    public static ByteBuffer newInstance(int initialSizeHint) {
        return newInstance(initialSizeHint, TimeZone.getDefault());
    }

    public static Buffer asBuffer(String str) {
        return newInstance(str.length() * 2).writeString(str).buffer;
    }

    private ByteBuffer(Buffer buffer, TimeZone timezone) {
        this.buffer = buffer != null ? buffer : Buffer.buffer();
        this.timezone = timezone == null ? TimeZone.getDefault() : timezone;
    }

    public int length() {
        // writerIndex of the inner Netty ByteBuf
        return this.buffer.length();
    }

    public boolean isExausted() {
        return this.position >= this.buffer.length();
    }

    public int readUnsignedLeb128() {
        int value = 0;
        int read;
        int count = 0;
        do {
            read = this.buffer.getByte(this.position++) & 0xff;
            value |= (read & 0x7f) << (count * 7);
            count++;
        } while (((read & 0x80) == 0x80) && count < 5);

        if ((read & 0x80) == 0x80) {
            throw new IllegalArgumentException("Invalid LEB128 sequence");
        }
        return value;
    }

    public ByteBuffer writeUnsignedLeb128(int value) {
        Utils.checkArgument(value, 0);

        int remaining = value >>> 7;
        while (remaining != 0) {
            this.buffer.appendByte((byte) ((value & 0x7f) | 0x80));
            value = remaining;
            remaining >>>= 7;
        }
        this.buffer.appendByte((byte) (value & 0x7f));

        return this;
    }

    public byte readByte() {
        return this.buffer.getByte(this.position++);
    }

    public ByteBuffer writeByte(byte value) {
        this.buffer.appendByte(value);

        return this;
    }

    public void readBytes(byte[] bytes) {
        readBytes(bytes, 0, bytes.length);
    }

    public void readBytes(byte[] bytes, int offset, int length) {
        byte[] readBytes = this.buffer.getBytes(this.position, this.position + length);
        this.position += length;

        System.arraycopy(readBytes, 0, bytes, 0, length);
    }

    public ByteBuffer writeBytes(byte[] value) {
        this.buffer.appendBytes(value);

        return this;
    }

    public boolean readBoolean() {
        byte value = this.readByte();

        Utils.checkArgument(value, 0, 1);
        return value == (byte) 1;
    }

    public ByteBuffer writeBoolean(boolean value) {
        return writeByte(value ? (byte) 1 : (byte) 0);
    }

    public ByteBuffer writeBoolean(byte value) {
        return writeByte(value == (byte) 1 ? (byte) 1 : (byte) 0);
    }

    public String readEnum() {
        return readString();
    }

    public String readEnum8() {
        return readString();
    }

    public String readEnum16() {
        return readString();
    }

    public ByteBuffer writeEnum(int value) {
        return writeEnum8(value);
    }

    public ByteBuffer writeEnum(String value) {
        return writeEnum8(value);
    }

    public ByteBuffer writeEnum8(byte value) {
        return writeInt8(value);
    }

    public ByteBuffer writeEnum8(int value) {
        return writeInt8(value);
    }

    public ByteBuffer writeEnum8(String value) {
        return writeString(value);
    }

    public ByteBuffer writeEnum16(int value) {
        return writeInt16(value);
    }

    public ByteBuffer writeEnum16(short value) {
        return writeInt16(value);
    }

    public ByteBuffer writeEnum16(String value) {
        return writeString(value);
    }

    /*
     * public boolean readIsNull() throws IOException { int value = readByte(); if
     * (value == -1) throw new EOFException();
     * 
     * validateInt(value, 0, 1, "nullable"); return value != 0; }
     */

    public boolean readNull() {
        return this.readBoolean();
    }

    public ByteBuffer writeNull() {
        return writeBoolean(true);
    }

    public ByteBuffer writeNonNull() {
        return writeBoolean(false);
    }

    public byte readInt8() {
        return this.readByte();
    }

    public ByteBuffer writeInt8(byte value) {
        return writeByte(value);
    }

    public ByteBuffer writeInt8(int value) {
        Utils.checkArgument(value, Byte.MIN_VALUE);

        return value > Byte.MAX_VALUE ? writeUInt8(value) : writeByte((byte) value);
    }

    public short readUInt8() {
        return (short) (this.readByte() & 0xFFL);
    }

    public ByteBuffer writeUInt8(int value) {
        Utils.checkArgument(value, 0, U_INT8_MAX);

        return writeByte((byte) (value & 0xFFL));
    }

    public short readInt16() {
        short value = this.buffer.getShortLE(this.position);

        this.position += 2;

        return value;
    }

    public ByteBuffer writeInt16(short value) {
        this.buffer.appendByte((byte) (0xFFL & value)).appendByte((byte) (0xFFL & (value >> 8)));
        return this;
    }

    public ByteBuffer writeInt16(int value) {
        Utils.checkArgument(value, Short.MIN_VALUE);

        return value > U_INT16_MAX ? writeUInt16(value) : writeInt16((short) value);
    }

    public int readUInt16() {
        return (int) (this.readInt16() & 0xFFFFL);
    }

    public ByteBuffer writeUInt16(int value) {
        Utils.checkArgument(value, 0, U_INT16_MAX);

        return writeInt16((short) (value & 0xFFFFL));
    }

    public int readInt32() {
        int value = this.buffer.getIntLE(this.position);

        this.position += 4;

        return value;
    }

    public ByteBuffer writeInt32(int value) {
        this.buffer.appendByte((byte) (0xFFL & value)).appendByte((byte) (0xFFL & (value >> 8)))
                .appendByte((byte) (0xFFL & (value >> 16))).appendByte((byte) (0xFFL & (value >> 24)));
        return this;
    }

    public long readUInt32() {
        return this.readInt32() & 0xFFFFFFFFL;
    }

    public ByteBuffer writeUInt32(long value) {
        Utils.checkArgument(value, 0, U_INT32_MAX);

        return writeInt32((int) (value & 0xFFFFFFFFL));
    }

    public long readInt64() {
        long value = this.buffer.getLongLE(this.position);

        this.position += 8;

        return value;
    }

    public ByteBuffer writeInt64(long value) {
        value = Long.reverseBytes(value);

        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (value & 0xFFL);
            value >>= 8;
        }

        return writeBytes(bytes);
    }

    public BigInteger readInt128() {
        byte[] r = new byte[16];
        for (int i = r.length - 1; i >= 0; i--) {
            r[i] = this.readByte();
        }

        return new BigInteger(r);
    }

    public ByteBuffer writeInt128(BigInteger value) {
        return writeBigInteger(value, DataType.Int128.getLength());
    }

    public BigInteger readInt256() {
        byte[] r = new byte[32];
        for (int i = r.length - 1; i >= 0; i--) {
            r[i] = this.readByte();
        }

        return new BigInteger(r);
    }

    public ByteBuffer writeInt256(BigInteger value) {
        return writeBigInteger(value, DataType.Int256.getLength());
    }

    public BigInteger readUInt64() {
        return new BigInteger(Long.toUnsignedString(this.readInt64()));
    }

    public ByteBuffer writeUInt64(long value) {
        Utils.checkArgument(value, 0);

        return writeInt64(value);
    }

    public ByteBuffer writeUInt64(BigInteger value) {
        Utils.checkArgument(value, BigInteger.ZERO);

        return writeInt64(value.longValue());
    }

    public BigInteger readUInt128() {
        return readInt128();
    }

    public ByteBuffer writeUInt128(BigInteger value) {
        return writeInt128(value);
    }

    public BigInteger readUInt256() {
        return readInt256();
    }

    public ByteBuffer writeUInt256(BigInteger value) {
        return writeInt256(value);
    }

    public float readFloat32() {
        return Float.intBitsToFloat(this.readInt32());
    }

    public ByteBuffer writeFloat32(float value) {
        return writeInt32(Float.floatToIntBits(value));
    }

    public double readFloat64() {
        return Double.longBitsToDouble(this.readInt64());
    }

    public ByteBuffer writeFloat64(double value) {
        return writeInt64(Double.doubleToLongBits(value));
    }

    public ByteBuffer writeUUID(java.util.UUID value) {
        return writeInt64(value.getMostSignificantBits()).writeInt64(value.getLeastSignificantBits());
    }

    public java.util.UUID readUUID() {
        return new java.util.UUID(readInt64(), readInt64());
    }

    public ByteBuffer writeBigInteger(BigInteger value) {
        return writeBigInteger(value, 16);
    }

    public ByteBuffer writeBigInteger(BigInteger value, int length) {
        byte empty = value.signum() == -1 ? (byte) 0xFF : 0x00;
        byte[] bytes = value.toByteArray();
        for (int i = bytes.length - 1; i >= 0; i--) {
            writeByte(bytes[i]);
        }

        // FIXME when the given (byte)length is less than bytes.length...
        for (int i = length - bytes.length; i > 0; i--) {
            writeByte(empty);
        }

        return this;
    }

    private BigInteger toBigInteger(BigDecimal value, int scale) {
        return value.multiply(BigDecimal.valueOf(10).pow(scale)).toBigInteger();
    }

    public BigDecimal readDecimal(int precision, int scale) {
        return precision > 38 ? readDecimal256(scale)
                : (precision > 18 ? readDecimal128(scale)
                        : (precision > 9 ? readDecimal64(scale) : readDecimal32(scale)));
    }

    public ByteBuffer writeDecimal(BigDecimal value, int precision, int scale) {
        return precision > 38 ? writeDecimal256(value, scale)
                : (precision > 18 ? writeDecimal128(value, scale)
                        : (precision > 9 ? writeDecimal64(value, scale) : writeDecimal32(value, scale)));
    }

    public BigDecimal readDecimal32(int scale) {
        return new BigDecimal(this.readInt32()).divide(BigDecimal.valueOf(10).pow(scale));
    }

    public ByteBuffer writeDecimal32(BigDecimal value, int scale) {
        return writeInt32(toBigInteger(value, scale).intValue());
    }

    public BigDecimal readDecimal64(int scale) {
        return new BigDecimal(this.readInt64()).divide(BigDecimal.valueOf(10).pow(scale));
    }

    public ByteBuffer writeDecimal64(BigDecimal value, int scale) {
        return writeInt64(toBigInteger(value, scale).longValue());
    }

    public BigDecimal readDecimal128(int scale) {
        byte[] r = new byte[16];
        for (int i = r.length - 1; i >= 0; i--) {
            r[i] = this.readByte();
        }

        return new BigDecimal(new BigInteger(r), scale);
    }

    public ByteBuffer writeDecimal128(BigDecimal value, int scale) {
        return writeInt128(toBigInteger(value, scale));
    }

    public BigDecimal readDecimal256(int scale) {
        byte[] r = new byte[32];
        for (int i = r.length - 1; i >= 0; i--) {
            r[i] = this.readByte();
        }

        return new BigDecimal(new BigInteger(r), scale);
    }

    public ByteBuffer writeDecimal256(BigDecimal value, int scale) {
        return writeInt256(toBigInteger(value, scale));
    }

    public Timestamp readDateTime() {
        return readDateTime(null);
    }

    public Timestamp readDateTime(TimeZone tz) {
        long time = this.readUInt32() * 1000L;

        if ((tz = tz == null ? this.timezone : tz) != null) {
            time -= tz.getOffset(time);
        }

        return new Timestamp(time <= 0L ? 1L : time);
    }

    public ByteBuffer writeDateTime(Date value) {
        return writeDateTime(value, null);
    }

    public ByteBuffer writeDateTime(Date value, TimeZone tz) {
        return writeDateTime(Objects.requireNonNull(value).getTime(), tz);
    }

    public ByteBuffer writeDateTime(Timestamp value, TimeZone tz) {
        // Objects.requireNonNull(value).getTime() - value.getTimezoneOffset() * 60 *
        // 1000L
        return writeDateTime(Objects.requireNonNull(value).getTime(), tz);
    }

    public ByteBuffer writeDateTime(long time, TimeZone tz) {
        if ((tz = tz == null ? this.timezone : tz) != null) {
            time += tz.getOffset(time);
        }

        if (time <= 0L) { // 0000-00-00 00:00:00
            time = 1L;
        } else if (time > DATETIME_MAX) { // 2106-02-07 06:28:15
            time = DATETIME_MAX;
        }

        time = time / 1000L;

        if (time > Integer.MAX_VALUE) {
            // https://github.com/google/guava/blob/master/guava/src/com/google/common/io/LittleEndianDataOutputStream.java#L130
            this.buffer.appendBytes(new byte[] { (byte) (0x0FFL & time), (byte) (0x0FFL & (time >> 8)),
                    (byte) (0x0FFL & (time >> 16)), (byte) (0x0FFL & (time >> 24)) });
        } else {
            writeUInt32((int) time);
        }

        return this;
    }

    public Timestamp readDateTime64() {
        return readDateTime64(null);
    }

    public Timestamp readDateTime64(TimeZone tz) {
        BigInteger time = this.readUInt64();

        if ((tz = tz == null ? this.timezone : tz) != null) {
            time = time.subtract(BigInteger.valueOf(tz.getOffset(time.longValue())));
        }

        if (time.compareTo(BigInteger.ZERO) < 0) { // 0000-00-00 00:00:00
            time = BigInteger.ONE;
        }

        return new Timestamp(time.longValue());
    }

    public ByteBuffer writeDateTime64(Date value, int scale) {
        return writeDateTime64(value, scale, null);
    }

    public ByteBuffer writeDateTime64(Timestamp value, int scale) {
        return writeDateTime64(value, scale, null);
    }

    public ByteBuffer writeDateTime64(Date value, int scale, TimeZone tz) {
        return writeDateTime64(Objects.requireNonNull(value).getTime(), 0, scale, tz);
    }

    public ByteBuffer writeDateTime64(Timestamp value, int scale, TimeZone tz) {
        return writeDateTime64(Objects.requireNonNull(value).getTime(), value.getNanos(), scale, tz);
    }

    // ClickHouse's DateTime64 supports precision from 0 to 18, but JDBC only
    // supports 3(millisecond)
    public ByteBuffer writeDateTime64(long time, int nanos, int scale, TimeZone tz) {
        if ((tz = tz == null ? this.timezone : tz) != null) {
            time += tz.getOffset(time);
        }

        if (time <= 0L) { // 0000-00-00 00:00:00.000
            time = nanos > 0 ? nanos / 1000000 : 1L;
        }

        if (scale > 0) {
            double normalizedTime = time;
            if (nanos != 0) {
                normalizedTime = time - nanos / 1000000 + nanos / 1000000.0;
            }

            if (scale < 3) {
                time = BigDecimal.valueOf(normalizedTime).divide(BigDecimal.valueOf(10).pow(3 - scale)).longValue();
            } else if (scale > 3) {
                time = BigDecimal.valueOf(normalizedTime).multiply(BigDecimal.valueOf(10).pow(scale - 3)).longValue();
            }
        }

        return this.writeUInt64(time);
    }

    public java.sql.Date readDate() {
        // long time = this.readUInt16() * MILLIS_IN_DAY;

        // TimeZone tz = this.timezone == null ? TimeZone.getDefault() : this.timezone;
        // time -= tz.getOffset(time);

        // return new Date(time <= 0L ? 1L : time);

        int daysSinceEpoch = this.readUInt16();
        return new java.sql.Date(daysSinceEpoch * MILLIS_IN_DAY);
    }

    public ByteBuffer writeDate(Date value) {
        Objects.requireNonNull(value);

        TimeZone tz = this.timezone == null ? TimeZone.getDefault() : this.timezone;
        long time = value.getTime();
        int daysSinceEpoch = (int) ((time + tz.getOffset(time)) / MILLIS_IN_DAY);

        // FIXME introduce strict mode to fix data issue only when needed
        return writeUInt16(daysSinceEpoch <= 0 ? 1 : (daysSinceEpoch > U_INT16_MAX ? U_INT16_MAX : daysSinceEpoch));
    }

    public String readFixedString(int length) {
        return readFixedString(length, StandardCharsets.UTF_8);
    }

    public String readFixedString(int length, Charset charset) {
        byte[] bytes = new byte[length];
        this.readBytes(bytes);

        return new String(bytes, charset == null ? StandardCharsets.UTF_8 : charset);
    }

    public ByteBuffer writeFixedString(String value, int length) {
        return writeFixedString(value, length, StandardCharsets.UTF_8);
    }

    public ByteBuffer writeFixedString(String value, int length, Charset charset) {
        byte[] src = value.getBytes(charset == null ? StandardCharsets.UTF_8 : charset);
        Utils.checkArgument(src, length);

        byte[] bytes = new byte[length];
        System.arraycopy(src, 0, bytes, 0, src.length);

        return writeBytes(bytes);
    }

    public String readString() {
        int length = this.readUnsignedLeb128();
        byte[] bytes = new byte[length];
        this.readBytes(bytes);

        return new String(bytes, StandardCharsets.UTF_8);
    }

    public ByteBuffer writeString(String value) {
        return writeString(value, false);
    }

    public ByteBuffer writeString(String value, boolean normalize) {
        Objects.requireNonNull(value);

        if (normalize) {
            int len = value.length();
            StringBuilder sb = new StringBuilder(len);
            for (int i = 0; i < len; i++) {
                char ch = value.charAt(i);
                switch (ch) {
                    // why? because it will ruin CSV file...
                    case '\r':
                    case '\n':
                        sb.append(' ');
                        break;

                    default:
                        sb.append(ch);
                        break;
                }
            }

            value = sb.toString();
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return writeUnsignedLeb128(bytes.length).writeBytes(bytes);
    }

    public ByteBuffer writeDefaultValue(ColumnDefinition column, DefaultValues defaultValues) {
        switch (column.getType()) {
            case Bool:
                writeInt8(defaultValues.Bool.getValue());
                break;
            case Int8:
                writeInt8(defaultValues.Int8.getValue());
                break;
            case Int16:
                writeInt16(defaultValues.Int16.getValue());
                break;
            case Int32:
                writeInt32(defaultValues.Int32.getValue());
                break;
            case Int64:
                writeInt64(defaultValues.Int64.getValue());
                break;
            case Int128:
                writeInt128(defaultValues.Int128.getValue());
                break;
            case Int256:
                writeInt256(defaultValues.Int256.getValue());
                break;
            case UInt8:
                writeUInt8(defaultValues.UInt8.getValue());
                break;
            case UInt16:
                writeUInt16(defaultValues.UInt16.getValue());
                break;
            case UInt32:
                writeUInt32(defaultValues.UInt32.getValue());
                break;
            case UInt64:
                writeUInt64(defaultValues.UInt64.getValue());
                break;
            case UInt128:
                writeUInt128(defaultValues.UInt128.getValue());
                break;
            case UInt256:
                writeUInt256(defaultValues.UInt256.getValue());
                break;
            case Float32:
                writeFloat32(defaultValues.Float32.getValue());
                break;
            case Float64:
                writeFloat64(defaultValues.Float64.getValue());
                break;
            case Date:
                writeUInt16(defaultValues.Date.getValue());
                break;
            case DateTime:
                writeUInt32(defaultValues.Datetime.getValue());
                break;
            case DateTime64:
                writeUInt64(defaultValues.Datetime64.getValue());
                break;
            case Decimal:
                writeDecimal(defaultValues.Decimal.getValue(), column.getPrecision(), column.getScale());
                break;
            case Decimal32:
                writeDecimal32(defaultValues.Decimal32.getValue(), column.getScale());
                break;
            case Decimal64:
                writeDecimal64(defaultValues.Decimal64.getValue(), column.getScale());
                break;
            case Decimal128:
                writeDecimal128(defaultValues.Decimal128.getValue(), column.getScale());
                break;
            case Decimal256:
                writeDecimal256(defaultValues.Decimal256.getValue(), column.getScale());
                break;
            case FixedStr:
                writeString(defaultValues.FixedStr.getValue());
                break;
            case Enum:
                writeInt8(defaultValues.Enum.getValue());
                break;
            case Enum8:
                writeInt8(defaultValues.Enum8.getValue());
                break;
            case Enum16:
                writeInt16(defaultValues.Enum16.getValue());
                break;
            case IPv4:
                writeUInt32(defaultValues.IPv4.getValue());
                break;
            case IPv6:
                writeString(defaultValues.IPv6.getValue());
                break;
            case Str:
                writeString(defaultValues.Str.getValue());
                break;
            case UUID:
                writeString(defaultValues.UUID.getValue());
                break;
            default:
                writeString(EMPTY_STRING);
                break;
        }

        return this;
    }

    public Buffer unwrap() {
        return this.buffer;
    }
}