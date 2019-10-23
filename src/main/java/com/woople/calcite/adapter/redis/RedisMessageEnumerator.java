package com.woople.calcite.adapter.redis;

import com.woople.calcite.adapter.redis.connection.RedisConnection;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.woople.calcite.adapter.redis.RedisTableConstants.SEDIS_REDIS_FETCHER_SIZE;

public class RedisMessageEnumerator implements Enumerator<Object[]> {
    private final static Logger logger = LoggerFactory.getLogger(RedisMessageEnumerator.class);
    private final RedisConnection redisConnection;
    private final AtomicBoolean cancelFlag;
    private final RedisTableOptions redisTableOptions;

    private Map<String, String> current;
    private ConcurrentLinkedQueue<Map<String, String>> bufferedRecords;

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


    public RedisMessageEnumerator(RedisConnection redisConnection,
                                  RedisTableOptions redisTableOptions,
                                  AtomicBoolean cancelFlag) {
        this.redisConnection = redisConnection;
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
            bufferedRecords = new ConcurrentLinkedQueue<>();
            pullRecords();
        }

        if (!bufferedRecords.isEmpty()) {
            current = bufferedRecords.poll();
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
        redisConnection.close();
    }

    private void pullRecords() {
        List<String> keys = this.redisConnection.keyCommands().keys(redisTableOptions.getPrefixKey() + "*", RedisTableConstants.REDIS_SCAN_COUNT);

        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

        int fetcherSize = Integer.parseInt(redisTableOptions.getParams().getOrDefault(SEDIS_REDIS_FETCHER_SIZE, "5"));

        CountDownLatch latch = new CountDownLatch((keys.size()-1)/fetcherSize + 1);

        List<String> keyList = new ArrayList<>();

        for (String key: keys){
            if (keyList.size() == fetcherSize){
                cachedThreadPool.execute(new RedissonFetcher(keyList , latch));
                keyList = new ArrayList<>();
            }

            keyList.add(key);
        }

        cachedThreadPool.execute(new RedissonFetcher(keyList, latch));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cachedThreadPool.shutdownNow();
        }

    }

    class RedissonFetcher implements Runnable{
        private List<String> keys;
        private CountDownLatch latch;

        public RedissonFetcher(List<String> keys, CountDownLatch latch) {
            this.keys = keys;
            this.latch = latch;
        }

        @Override
        public void run() {
            bufferedRecords.addAll(redisConnection.hashCommands().hGetAll(keys));
            latch.countDown();
        }
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
