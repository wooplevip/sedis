package com.woople.calcite.adapter.redis;

import com.google.common.collect.Lists;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.redisson.api.*;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class RedisMessageEnumerator implements Enumerator<Object[]> {
    private final RedissonClient redissonClient;
    private final AtomicBoolean cancelFlag;
    private final RedisTableOptions redisTableOptions;

    private Map<String, String> current;
    private LinkedList<Map<String, String>> bufferedRecords;

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP =
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }


    public RedisMessageEnumerator(RedissonClient redissonClient,
                                  RedisTableOptions redisTableOptions,
                                  AtomicBoolean cancelFlag) {
        this.redissonClient = redissonClient;
        this.cancelFlag = cancelFlag;
        this.redisTableOptions = redisTableOptions;
    }


    @Override
    public Object[] current() {
        return this.redisTableOptions.getRowConverter().toRow(current, redisTableOptions.getFields());
    }

    @Override
    public boolean moveNext() {
        if (cancelFlag.get()) {
            return false;
        }

        if (bufferedRecords == null){
            bufferedRecords = new LinkedList<>();
            pullRecords();
        }

        if (!bufferedRecords.isEmpty()) {
            current = bufferedRecords.removeFirst();
            return true;
        }

        return false;
    }

    @Override
    public void reset() {
        bufferedRecords = null;
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        bufferedRecords = null;
        redissonClient.shutdown();
    }

    private void pullRecords() {
        RBatch batch = redissonClient.createBatch(BatchOptions.defaults());
        Iterable<String> keys = redissonClient.getKeys().getKeysByPattern(redisTableOptions.getPrefixKey() + "*", RedisTableConstants.REDIS_SCAN_COUNT);
        List<String> keyList = Lists.newArrayList(keys);

        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

        int recordCount = 0;
        ConcurrentHashMap<String, RFuture<Map<String, String>>> rf = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(keyList.size());

        for (String key : keyList) {
            recordCount ++;
            if (recordCount > RedisTableConstants.REDIS_TABLE_RECORD_MAX){
                break;
            }
            RMapAsync<String, String> rMapAsync = batch.getMap(key);

            cachedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    RFuture<Map<String, String>> rfMap = rMapAsync.readAllMapAsync();
                    rf.put(key, rfMap);
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cachedThreadPool.shutdownNow();
        }

        for (String key : rf.keySet()) {
            RFuture<Map<String, String>> bazFuture = rf.get(key);
            bazFuture.whenComplete(new BiConsumer<Map<String, String>, Throwable>() {
                @Override
                public void accept(Map<String, String> objectObjectMap, Throwable throwable) {
                    Map<String, String> row = new HashMap<>(objectObjectMap);
                    row.put(redisTableOptions.getKeyFields()[0], StringUtils.substringAfter(key, ":"));
                    bufferedRecords.addLast(row);
                }
            });
        }

        batch.execute();
    }



    /** Row converter.
     *
     * @param <E> element type */
    abstract static class RowConverter<E> {
        abstract E convertRow(String[] rows);

        protected Object convert(RedisFieldType fieldType, String string) {
            if (fieldType == null) {
                return string;
            }
            switch (fieldType) {
                case BOOLEAN:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Boolean.parseBoolean(string);
                case BYTE:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Byte.parseByte(string);
                case SHORT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Short.parseShort(string);
                case INT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Integer.parseInt(string);
                case LONG:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Long.parseLong(string);
                case FLOAT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Float.parseFloat(string);
                case DOUBLE:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Double.parseDouble(string);
                case DATE:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_DATE.parse(string);
                        return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                    } catch (ParseException e) {
                        return null;
                    }
                case TIME:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIME.parse(string);
                        return (int) date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case TIMESTAMP:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                        return date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case STRING:
                default:
                    return string;
            }
        }
    }

    /** Array row converter. */
    static class ArrayRowConverter extends RowConverter<Object[]> {
        private final RedisFieldType[] fieldTypes;
        private final int[] fields;
        // whether the row to convert is from a stream
        private final boolean stream;

        ArrayRowConverter(List<RedisFieldType> fieldTypes, int[] fields) {
            this.fieldTypes = fieldTypes.toArray(new RedisFieldType[0]);
            this.fields = fields;
            this.stream = false;
        }

        ArrayRowConverter(List<RedisFieldType> fieldTypes, int[] fields, boolean stream) {
            this.fieldTypes = fieldTypes.toArray(new RedisFieldType[0]);
            this.fields = fields;
            this.stream = stream;
        }

        public Object[] convertRow(String[] strings) {
            if (stream) {
                return convertStreamRow(strings);
            } else {
                return convertNormalRow(strings);
            }
        }

        public Object[] convertNormalRow(String[] strings) {
            final Object[] objects = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                int field = fields[i];
                objects[i] = convert(fieldTypes[field], strings[field]);
            }
            return objects;
        }

        public Object[] convertStreamRow(String[] strings) {
            final Object[] objects = new Object[fields.length + 1];
            objects[0] = System.currentTimeMillis();
            for (int i = 0; i < fields.length; i++) {
                int field = fields[i];
                objects[i + 1] = convert(fieldTypes[field], strings[field]);
            }
            return objects;
        }
    }
}
