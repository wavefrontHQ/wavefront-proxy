/*
 * Copyright (c) 2019 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.wavefront.agent.logforwarder.ingestion.processors.util;


import com.vmware.xenon.common.Utils;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.Event;
import com.wavefront.agent.logforwarder.ingestion.processors.model.event.EventPayload;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class JsonUtils {

    public static final byte[] NULL_CHARS = new byte[] {'n' & 0xFF,'u' & 0xFF,'l' & 0xFF,'l' & 0xFF};
    public static final char[] HEX_CHARS = new char[] {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    public static final byte VALUE_SEPARATOR = ',' & 0xFF;
    public static final byte NAME_SEPARATOR = ':' & 0xFF;
    public static final byte OBJECT_START = '{' & 0xFF;
    public static final byte OBJECT_END = '}' & 0xFF;
    public static final byte ARRAY_START = '[' & 0xFF;
    public static final byte ARRAY_END = ']' & 0xFF;
    public static final byte QUOTE = '\"' & 0xFF;
    public static final byte BACKSLASH = '\\' & 0xFF;

    public static CustomByteArrayOutputStream toJsonByteStream(EventPayload payload) {

        List<Event> batch = payload.batch;

        CustomByteArrayOutputStream baos = new CustomByteArrayOutputStream((int)payload.payloadSizeInBytes);
        writeEventBatch(batch, baos);

        return baos;
    }

    private static void writeEventBatch(List<Event> batch, CustomByteArrayOutputStream baos) {
        write(ARRAY_START, baos);

        Iterator<Event> eventIterator = batch.iterator();
        while (eventIterator.hasNext()) {
            Map<String, Object> event = eventIterator.next();

            write(event, baos);

            if (eventIterator.hasNext()) {
                write(VALUE_SEPARATOR, baos);
            }
        }

        write(ARRAY_END, baos);
    }

    private static void write(List<?> batch, CustomByteArrayOutputStream baos) {
        write(ARRAY_START, baos);

        Iterator<?> eventIterator = batch.iterator();
        while (eventIterator.hasNext()) {
            Object event = eventIterator.next();

            writeValue(event, baos);

            if (eventIterator.hasNext()) {
                write(VALUE_SEPARATOR, baos);
            }
        }

        write(ARRAY_END, baos);
    }

    private static void write(Map<?, ?> event, CustomByteArrayOutputStream baos) {
        write(OBJECT_START, baos);

        Iterator<? extends Entry<?, ?>> entryIterator = event.entrySet().iterator();
        while (entryIterator.hasNext()) {

            Entry<?, ?> entry = entryIterator.next();

            // key
            writeQuoted(entry.getKey().toString(), baos);

            write(NAME_SEPARATOR, baos);

            // Value
            Object o = entry.getValue();
            writeValue(o, baos);


            if (entryIterator.hasNext()) {
                write(VALUE_SEPARATOR, baos);
            }
        }

        write(OBJECT_END, baos);
    }

    private static void writeValue(Object o, CustomByteArrayOutputStream baos) {
        // Simplifying serialization. We know only these types can be there
        if (o instanceof String) {
            writeQuoted(o.toString(), baos);
        } else if (o instanceof Number) {
            write(o.toString(), baos);
        } else if (o instanceof Boolean) {
            write(o.toString(), baos);
        } else if (o instanceof Map) {
            write((Map<?, ?>) o, baos);
        } else if (o instanceof Collection) {
            write((List<?>)o, baos);
        } else if (o == null) {
            writeNull(baos);
        } else {
            write(Utils.toJson(o), baos);
        }
    }

    private static void write(char o, CustomByteArrayOutputStream baos) {
        baos.write((o) & 0xFF);
    }

    public static void unicodeEscape(int ch, CustomByteArrayOutputStream baos) {
        write('\\', baos);
        write('u', baos);
        write(HEX_CHARS[ch >>> 12], baos);
        write(HEX_CHARS[(ch >>> 8) & 0xf], baos);
        write(HEX_CHARS[(ch >>> 4) & 0xf], baos);
        write(HEX_CHARS[ch & 0xf], baos);
    }

    private static void write(byte o, CustomByteArrayOutputStream baos) {
        baos.write(o);
    }

    private static void writeNull(CustomByteArrayOutputStream baos) {
        for (byte trueChar : NULL_CHARS) {
            write(trueChar, baos);
        }
    }

    private static void write(String o, CustomByteArrayOutputStream baos) {
        byte[] bytes = o.getBytes(StandardCharsets.UTF_8);
        for (byte cp : bytes) {
            if (cp > 31) {
                if (cp == 34 || cp == 92) {
                    write(BACKSLASH, baos);
                }
                baos.write(cp);
            } else if (cp == 13) {    // \r
                baos.write(BACKSLASH);
                baos.write('r');
            } else if (cp == 10) {    // \n
                baos.write(BACKSLASH);
                baos.write('n');
            } else if (cp == 9) {    // \t
                baos.write(BACKSLASH);
                baos.write('t');
            } else if (cp == 8) {    // \b
                baos.write(BACKSLASH);
                baos.write('b');
            } else if (cp == 12) {    // \f
                baos.write(BACKSLASH);
                baos.write('f');
            } else if (cp < 0) {
                baos.write(cp);
            } else {
                unicodeEscape(cp, baos);
            }
        }
    }

    private static void writeQuoted(String o, CustomByteArrayOutputStream baos) {
        write(QUOTE, baos);
        write(o, baos);
        write(QUOTE, baos);
    }


    public static class CustomByteArrayOutputStream extends ByteArrayOutputStream {

        public CustomByteArrayOutputStream(int size) {
            super(size);
        }

        public byte[] getBuf() {
            return buf;
        }

        public int getLength() {
            return count;
        }

        public void reset() {
            this.count = 0;
        }

        public void setBuf(byte[] buf) {
            this.buf = buf;
            this.count = buf.length;
        }

    }

}
