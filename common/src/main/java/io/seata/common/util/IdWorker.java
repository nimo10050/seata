/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.common.util;

import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author funkye
 * @author selfishlover
 */
public class IdWorker {

    /**
     * Start time cut (2020-05-03)
     */
    private final long twepoch = 1588435200000L;

    /**
     * The number of bits occupied by workerId
     */
    private final int workerIdBits = 10;

    /**
     * The number of bits occupied by timestamp
     */
    private final int timestampBits = 41;

    /**
     * The number of bits occupied by sequence
     */
    private final int sequenceBits = 12;

    /**
     * Maximum supported machine id, the result is 1023
     * 1.    -1 左移 10 位 变成 - 2 ^ 10
     * 2.    -1024 取反分两步
     *  2.1     1024 取反 + 1   变为了 1000 0000 000
     *  2.2     再按位取反             0111 1111 111
     * 即 1023
     */
    private final int maxWorkerId = ~(-1 << workerIdBits);

    /**
     * business meaning: machine ID (0 ~ 1023)
     * actual layout in memory:
     * highest 1 bit: 0
     * middle 10 bit: workerId
     * lowest 53 bit: all 0
     */
    private long workerId;

    /**
     * timestamp and sequence mix in one Long
     * highest 11 bit: not used
     * middle  41 bit: timestamp
     * lowest  12 bit: sequence
     */
    private AtomicLong timestampAndSequence;

    /**
     * mask that help to extract timestamp and sequence from a long
     * -1 左移 53 位， 变成 -2 ^ 53
     * 1000000000 0000000000 0000000000 0000000000 0000000000 0000
     * 取反后， 2 ^ 53 - 1
     * 0111111111 1111111111 1111111111 1111111111 1111111111 1111
     */
    private final long timestampAndSequenceMask = ~(-1L << (timestampBits + sequenceBits));

    /**
     * instantiate an IdWorker using given workerId
     * @param workerId if null, then will auto assign one
     */
    public IdWorker(Long workerId) {
        initTimestampAndSequence();
        initWorkerId(workerId);
    }

    /**
     * init first timestamp and sequence immediately
     */
    private void initTimestampAndSequence() {
        long timestamp = getNewestTimestamp();
        long timestampWithSequence = timestamp << sequenceBits;
        this.timestampAndSequence = new AtomicLong(timestampWithSequence);
    }

    /**
     * init workerId
     * @param workerId if null, then auto generate one
     */
    private void initWorkerId(Long workerId) {
        if (workerId == null) {
            workerId = generateWorkerId();
        }
        if (workerId > maxWorkerId || workerId < 0) {
            String message = String.format("worker Id can't be greater than %d or less than 0", maxWorkerId);
            throw new IllegalArgumentException(message);
        }
        // 左移 53 位
        this.workerId = workerId << (timestampBits + sequenceBits);
    }

    /**
     * get next UUID(base on snowflake algorithm), which look like:
     * highest 1 bit: always 0
     * next   10 bit: workerId
     * next   41 bit: timestamp
     * lowest 12 bit: sequence
     * @return UUID
     */
    public long nextId() {
        waitIfNecessary();
        // 41 位 时间戳 + 12 位 序列号
        long next = timestampAndSequence.incrementAndGet();
        // 取低 53 位
        long timestampWithSequence = next & timestampAndSequenceMask;
        // workId 已经左移的 53 位, 这里做了一个 异或，
        // 相当于 workerId 的对应的11位二进制 字符串 + timestampWithSequence 对应的 53 位二进制字符串
        // 最后的结果刚好 64 位二进制
        return workerId | timestampWithSequence;
    }

    /**
     * block current thread if the QPS of acquiring UUID is too high
     * that current sequence space is exhausted
     *
     * qps 过高时， 休眠 5 ms
     *
     */
    private void waitIfNecessary() {
        // 比如当前需要生成一个 id，进来后：

        // 首先需要了解一个前提：
        // seata 的全局 id 组成共 64 位二进制  11 位 workId + 41 位 时间戳（ms） + 12 位序列号
        // 序列号从 0 开始计数。 当序列号从 0 加到 4096 时, 会向前进位。 即时间戳 + 1 ms
        // seata 改良后的雪花算法, 只会在初始时, 拿到当前机器的时间戳。作为初始值， 之后不会依赖系统的时间。

        // 1. 先拿到上次的值 41 位 时间戳 + 12 位 序列号
        long currentWithSequence = timestampAndSequence.get();
        // 2. 右移 12 位。把 12 位序列号抹去， 留下时间戳
        long current = currentWithSequence >>> sequenceBits;
        // 3. 拿到最新的时间
        long newest = getNewestTimestamp();
        // 4. 上次的时间 大于 当前的时间。  这意味这什么???
        // 上次生成的序列号 超过了 4096 个。
        if (current >= newest) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException ignore) {
                // don't care
            }
        }
    }

    /**
     * get newest timestamp relative to twepoch
     */
    private long getNewestTimestamp() {
        return System.currentTimeMillis() - twepoch;
    }

    /**
     * auto generate workerId, try using mac first, if failed, then randomly generate one
     * @return workerId
     */
    private long generateWorkerId() {
        try {
            return generateWorkerIdBaseOnMac();
        } catch (Exception e) {
            return generateRandomWorkerId();
        }
    }

    /**
     * use lowest 10 bit of available MAC as workerId
     * @return workerId
     * @throws Exception when there is no available mac found
     */
    private long generateWorkerIdBaseOnMac() throws Exception {
        Enumeration<NetworkInterface> all = NetworkInterface.getNetworkInterfaces();
        while (all.hasMoreElements()) {
            NetworkInterface networkInterface = all.nextElement();
            boolean isLoopback = networkInterface.isLoopback();
            boolean isVirtual = networkInterface.isVirtual();
            if (isLoopback || isVirtual) {
                continue;
            }
            byte[] mac = networkInterface.getHardwareAddress();
            return ((mac[4] & 0B11) << 8) | (mac[5] & 0xFF);
        }
        throw new RuntimeException("no available mac found");
    }

    /**
     * randomly generate one as workerId
     * @return workerId
     */
    private long generateRandomWorkerId() {
        return new Random().nextInt(maxWorkerId + 1);
    }

    public static void main(String[] args) {
        long l = new IdWorker(100L).nextId();
        System.out.println(l);
    }
}
