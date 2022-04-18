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
import static org.testng.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.TimeZone;

import org.testng.annotations.Test;

public class ByteBufferTest {
    @Test(groups = { "unit" })
    public void testWriteBoolean() {
        ByteBuffer buffer = ByteBuffer.newInstance(100);

        buffer.writeBoolean(true);
        buffer.writeBoolean(false);
        buffer.writeBoolean((byte) 1);
        buffer.writeBoolean((byte) 0);
        buffer.writeBoolean((byte) -1);
        buffer.writeBoolean((byte) 2);
        assertEquals(buffer.buffer.getBytes(),
                new byte[] { (byte) 1, (byte) 0, (byte) 1, (byte) 0, (byte) 0, (byte) 0 });
    }

    @Test(groups = { "unit" })
    public void testWriteEnum8() {
        ByteBuffer buffer = ByteBuffer.newInstance(100);

        buffer.writeEnum("A");
        buffer.writeEnum("");
        buffer.writeEnum(-1);
        buffer.writeEnum(0);
        buffer.writeEnum(1);
        buffer.writeEnum8("B");
        buffer.writeEnum8("");
        buffer.writeEnum8(-128);
        buffer.writeEnum8(0);
        buffer.writeEnum8(128);
        assertEquals(buffer.buffer.getBytes(), new byte[] { (byte) 1, (byte) 65, (byte) 0, (byte) -1, (byte) 0,
                (byte) 1, (byte) 1, (byte) 66, (byte) 0, (byte) -128, (byte) 0, (byte) 128 });
    }

    @Test(groups = { "unit" })
    public void testWriteEnum16() {
        ByteBuffer buffer = ByteBuffer.newInstance(100);

        buffer.writeEnum16("A");
        buffer.writeEnum16("");
        buffer.writeEnum16(-32768);
        buffer.writeEnum16(0);
        buffer.writeEnum16(32768);
        assertEquals(buffer.buffer.getBytes(), new byte[] { (byte) 1, (byte) 65, (byte) 0, (byte) 0, (byte) -128,
                (byte) 0, (byte) 0, (byte) 0, (byte) 128 });
    }

    @Test(groups = { "unit" })
    public void testWriteInt8() {
        ByteBuffer buffer = ByteBuffer.newInstance(100);

        buffer.writeInt8(Byte.MIN_VALUE);
        buffer.writeInt8(Byte.MAX_VALUE);
        buffer.writeInt8(0xff);
        assertEquals(buffer.buffer.getBytes(), new byte[] { Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0xff });
    }

    @Test(groups = { "unit" })
    public void testWriteFixedString() {
        ByteBuffer buffer = ByteBuffer.newInstance(100);

        buffer.writeFixedString("A", 2);
        assertEquals(buffer.buffer.getBytes(), new byte[] { (byte) 65, (byte) 0 });
        buffer.writeFixedString("A", 1);
        assertEquals(buffer.buffer.getBytes(), new byte[] { (byte) 65, (byte) 0, (byte) 65 });
        assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
            @Override
            public void run() throws Throwable {
                buffer.writeFixedString("AA", 1);
            }
        });
    }

    @Test(groups = { "unit" })
    public void testWriteString() {
        ByteBuffer buffer = ByteBuffer.newInstance(100);

        buffer.writeString("A");
        assertEquals(buffer.buffer.getBytes(), new byte[] { (byte) 1, (byte) 65 });
        // assertEquals(buffer.readString(), str);
    }

    @Test(groups = { "unit" })
    public void testWriteAndRead() {
        ByteBuffer buffer = ByteBuffer.newInstance(100);

        byte b1 = Byte.MAX_VALUE;
        byte b2 = Byte.MIN_VALUE;
        byte[] bytes1 = new byte[] { b2, b1 };
        byte[] bytes2 = new byte[] { b2, b1 };
        short s1 = Short.MAX_VALUE;
        short s2 = Short.MIN_VALUE;
        int i1 = Integer.MAX_VALUE;
        int i2 = Integer.MIN_VALUE;
        long l1 = Long.MAX_VALUE;
        long l2 = Long.MIN_VALUE;
        float f1 = Float.MAX_VALUE;
        float f2 = Float.MIN_VALUE;
        double d1 = Double.MAX_VALUE;
        double d2 = Double.MIN_VALUE;
        String str1 = "";
        String str2 = "1\r2\n3\ta,b.c;:$@^\\&*(!@#%`~-+=|[]{}'\"?/<>)";
        Date date1 = new Date(MILLIS_IN_DAY);
        Date date2 = new Date(DATETIME_MAX / MILLIS_IN_DAY * MILLIS_IN_DAY);
        java.sql.Timestamp dt1 = new java.sql.Timestamp(MILLIS_IN_DAY + 1000L);
        java.sql.Timestamp dt2 = new java.sql.Timestamp(DATETIME_MAX / MILLIS_IN_DAY * MILLIS_IN_DAY + 1000L);
        java.sql.Timestamp xdt1 = new java.sql.Timestamp(MILLIS_IN_DAY + 1L);
        java.sql.Timestamp xdt2 = new java.sql.Timestamp(DATETIME_MAX / MILLIS_IN_DAY * MILLIS_IN_DAY + 1L);

        buffer.writeNull();
        buffer.writeNonNull();
        buffer.writeByte(b1);
        buffer.writeByte(b2);
        // buffer.writeBytes(bytes1);
        // buffer.writeBytes(bytes2);
        buffer.writeInt8(b1);
        buffer.writeInt8(b2);
        buffer.writeUInt8(b1 + 1);
        buffer.writeUInt8(Math.abs(b2) - 1);
        buffer.writeInt16(s1);
        buffer.writeInt16(s2);
        buffer.writeUInt16(s1 + 1);
        buffer.writeUInt16(Math.abs(s2) - 1);
        buffer.writeInt32(i1);
        buffer.writeInt32(i2);
        buffer.writeUInt32(i1 + 1L);
        buffer.writeUInt32(i2 * -1L - 1L);
        buffer.writeInt64(l1);
        buffer.writeInt64(l2);
        buffer.writeUInt64(l1);
        buffer.writeUInt64(BigInteger.valueOf(l1).add(BigInteger.ONE));
        buffer.writeUInt64(BigInteger.valueOf(l2).multiply(BigInteger.valueOf(-1L)).subtract(BigInteger.ONE));
        buffer.writeFloat32(f1);
        buffer.writeFloat32(f2);
        buffer.writeFloat64(d1);
        buffer.writeFloat64(d2);
        buffer.writeDecimal(BigDecimal.valueOf(l1), 19, 4);
        buffer.writeString(str1);
        buffer.writeString(str2);
        buffer.writeDate(date1);
        buffer.writeDate(date2);
        buffer.writeDateTime(dt1);
        buffer.writeDateTime(dt2, TimeZone.getTimeZone("UTC"));
        buffer.writeDateTime64(xdt1, 3);
        buffer.writeDateTime64(xdt2, 3);

        assertEquals(buffer.readNull(), true);
        assertEquals(buffer.readNull(), false);
        assertEquals(buffer.readByte(), b1);
        assertEquals(buffer.readByte(), b2);
        // buffer.readBytes(bytes1);
        // assertEquals(bytes1, bytes2);
        // buffer.readBytes(bytes2, 0, bytes2.length);
        // assertEquals(bytes2, bytes1);
        assertEquals(buffer.readInt8(), b1);
        assertEquals(buffer.readInt8(), b2);
        assertEquals(buffer.readUInt8(), b1 + 1);
        assertEquals(buffer.readUInt8(), Math.abs(b2) - 1);
        assertEquals(buffer.readInt16(), s1);
        assertEquals(buffer.readInt16(), s2);
        assertEquals(buffer.readUInt16(), s1 + 1);
        assertEquals(buffer.readUInt16(), Math.abs(s2) - 1);
        assertEquals(buffer.readInt32(), i1);
        assertEquals(buffer.readInt32(), i2);
        assertEquals(buffer.readUInt32(), i1 + 1L);
        assertEquals(buffer.readUInt32(), i2 * -1L - 1L);
        assertEquals(buffer.readInt64(), l1);
        assertEquals(buffer.readInt64(), l2);
        assertEquals(buffer.readUInt64().longValue(), l1);
        assertEquals(buffer.readUInt64(), BigInteger.valueOf(l1).add(BigInteger.ONE));
        assertEquals(buffer.readUInt64(),
                BigInteger.valueOf(l2).multiply(BigInteger.valueOf(-1L)).subtract(BigInteger.ONE));
        assertEquals(buffer.readFloat32(), f1);
        assertEquals(buffer.readFloat32(), f2);
        assertEquals(buffer.readFloat64(), d1);
        assertEquals(buffer.readFloat64(), d2);
        assertEquals(buffer.readDecimal(19, 4), BigDecimal.valueOf(l1, 4).multiply(BigDecimal.valueOf(10000L)));
        assertEquals(buffer.readString(), str1);
        assertEquals(buffer.readString(), str2);
        assertEquals(buffer.readDate(), date1);
        assertEquals(buffer.readDate(), date2);
        assertEquals(buffer.readDateTime(), dt1);
        assertEquals(buffer.readDateTime(TimeZone.getTimeZone("UTC")), dt2);
        assertEquals(buffer.readDateTime64(), xdt1);
        assertEquals(buffer.readDateTime64(), xdt2);
    }
}
