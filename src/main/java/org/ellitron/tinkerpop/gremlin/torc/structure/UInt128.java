/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ellitron.tinkerpop.gremlin.torc.structure;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * Container for a 128 bit unsigned integer stored in big-endian format. This
 * class is not intended to be used for doing arithmetic, comparison, or logical
 * operations and hence has no such methods for doing so. For that see
 * {@link java.math.BigInteger}. Its primary use is to convert other number
 * representations to their 128 bit unsigned representation, including Strings
 * and BigIntegers, and return a byte array of length 16 that contains the
 * unsigned representation.
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class UInt128 {

    public static final int SIZE = 128;
    public static final int BYTES = SIZE / Byte.SIZE;

    private final long upperLong;
    private final long lowerLong;

    /**
     * Constructs a UInt128 from a Byte value. Value is treated as unsigned and
     * therefore no sign extension is performed.
     *
     * @param val Byte, treated as 8 unsigned bits.
     */
    public UInt128(Byte val) {
        this(0, ((long) val) & 0x00000000000000FFL);
    }

    /**
     * Constructs a UInt128 from a Short value. Value is treated as unsigned and
     * therefore no sign extension is performed.
     *
     * @param val Short, treated as 16 unsigned bits.
     */
    public UInt128(Short val) {
        this(0, ((long) val) & 0x000000000000FFFFL);
    }

    /**
     * Constructs a UInt128 from an Integer value. Value is treated as unsigned
     * and therefore no sign extension is performed.
     *
     * @param val Integer, treated as 32 unsigned bits.
     */
    public UInt128(Integer val) {
        this(0, ((long) val) & 0x00000000FFFFFFFFL);
    }

    /**
     * Constructs a UInt128 from a Long value. Value is treated as unsigned and
     * therefore no sign extension is performed.
     *
     * @param val Long, treated as 64 unsigned bits.
     */
    public UInt128(Long val) {
        this(0, val);
    }

    /**
     * Constructs a UInt128 from a String value representing a number in base
     * 10. The value stored in this UInt128 is the 128 bit two's-complement
     * representation of this value. If the two's-complement representation of
     * the value requires more than 128 bits to represent, only the lower 128
     * bits are used to construct this UInt128.
     *
     * @param val Base 10 format String, treated as a 128 bit signed value.
     */
    public UInt128(String val) {
        this(val, 10);
    }

    /**
     * Constructs a UInt128 from a String value representing a number in a specified base. 
     * The value stored in this UInt128 is the 128 bit two's-complement
     * representation of this value. If the two's-complement representation of
     * the value requires more than 128 bits to represent, only the lower 128
     * bits are used to construct this UInt128.
     * 
     * TODO: does this work?
     * {@inheritDoc UInt128(String)}
     *
     * @param val String, treated as a 128 bit signed value.
     * @param radix The base of the number represented by the string.
     */
    public UInt128(String val, int radix) {
        this(new BigInteger(val, radix));
    }

    /**
     * Constructs a UInt128 from a BigInteger value. The value used to construct
     * the UInt128 is the 128-bit two's complement representation of the
     * BigInteger. In the case that the number of bits in the minimal
     * two's-complement representation of the BigInteger is greater than 128,
     * then the lower 128 bits are used and higher order bits are discarded.
     *
     * @param val BigInteger, treated as a 128 bit signed value.
     */
    public UInt128(BigInteger val) {
        byte[] valArray = val.toByteArray();
        byte[] resArray = new byte[BYTES];

        if (valArray.length >= BYTES) {
            resArray = Arrays.copyOfRange(valArray, valArray.length - BYTES, valArray.length);
        } else {
            byte pad = 0x00;
            if (valArray[0] < 0) {
                pad = (byte) 0xFF;
            }

            for (int i = 0; i < BYTES; i++) {
                if (i < (BYTES - valArray.length)) {
                    resArray[i] = pad;
                } else {
                    resArray[i] = valArray[i - (BYTES - valArray.length)];
                }
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(BYTES);
        buf.put(resArray);
        buf.flip();
        this.upperLong = buf.getLong();
        this.lowerLong = buf.getLong();
    }

    public UInt128(UUID val) {
        this.upperLong = val.getMostSignificantBits();
        this.lowerLong = val.getLeastSignificantBits();
    }
    
    /**
     * Constructs a UInt128 from a byte array value. The byte array value is
     * interpreted as unsigned and in big-endian format. If the byte array is
     * less than 16 bytes, then the higher order bytes of the resulting UInt128
     * are padded with 0s. If the byte array is greater than 16 bytes, then the
     * lower 16 bytes are used.
     *
     * @param val Byte array in big-endian format.
     */
    public UInt128(byte[] val) {
        byte[] res = new byte[BYTES];

        if (val.length >= BYTES) {
            res = Arrays.copyOfRange(val, val.length - BYTES, val.length);
        } else {
            for (int i = 0; i < BYTES; i++) {
                if (i < (BYTES - val.length)) {
                    res[i] = 0;
                } else {
                    res[i] = val[i - (BYTES - val.length)];
                }
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(BYTES);
        buf.put(res);
        buf.flip();
        this.upperLong = buf.getLong();
        this.lowerLong = buf.getLong();
    }

    public UInt128(final long upperLong, final long lowerLong) {
        this.upperLong = upperLong;
        this.lowerLong = lowerLong;
    }

    public long getUpperLong() {
        return upperLong;
    }

    public long getLowerLong() {
        return lowerLong;
    }

    public byte[] toByteArray() {
        ByteBuffer buf = ByteBuffer.allocate(BYTES);
        buf.putLong(upperLong);
        buf.putLong(lowerLong);
        return buf.array();
    }
    
    @Override
    public String toString() {
        if (upperLong == 0) {
            return String.format("0x%X", lowerLong);
        } else {
            return String.format("0x%X%016X", upperLong, lowerLong);
        }
    }
}
