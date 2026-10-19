/*
 * Copyright (C) 2017-2026 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.swarmchestrate.ems.client.filter;

import gr.iccs.imu.ems.api.client.IMetricsFilter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class AdaptiveMetricsFilter implements IMetricsFilter, InitializingBean {

    // FIFO to keep last 2 values per metric/node (store value + timestamp)
    private final Map<String, Deque<MetricSample>> metricHistory = new ConcurrentHashMap<>();
    // track last time a metric was published (milliseconds since epoch)
    private final Map<String, Long> lastPublishedAt = new ConcurrentHashMap<>();
    private final double PERCENTAGE_THRESHOLD = 10.0;

    private static class MetricSample {
        final double value;
        final long timestamp;

        MetricSample(double value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "MetricSample(value=" + value + ", ts=" + timestamp + ")";
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info(">>>>>>>>>>>>>>>>> AdaptiveMetricsFilter initialized");
    }

    @Override
    public Context prePublishMetric(String metricName, double metricValue, long timestamp, String nodeAddress) {
        log.info(">>>>>>>>>>>>>>>>> AdaptiveMetricsFilter prePublishMetric: name={}, value={}, ts={}, nodeAddress={}", metricName, metricValue, timestamp, nodeAddress);

        boolean canPublish = doFilter(metricName, metricValue, timestamp, nodeAddress);

        return new Context(canPublish, timestamp);
    }

    private boolean doFilter(String metricName, double metricValue, long timestamp, String nodeAddress) {
        String historyKey = metricName + "@" + nodeAddress;
        metricHistory.putIfAbsent(historyKey, new ArrayDeque<>(2));
        Deque<MetricSample> history = metricHistory.get(historyKey);
        if (history.size() == 2) {
            MetricSample prevSample = history.peekFirst();
            double prev = prevSample.value;
            if (prev == 0) {
                if (metricValue == 0) {
                    log.info("NetdataCollector: Previous value is 0 and current value is also 0 for metric {} on node {}. Skipping publish. (timestamp={})", metricName, nodeAddress, timestamp);
                    history.removeFirst();
                    history.addLast(new MetricSample(metricValue, timestamp));
                    return false;
                }
                // prev == 0, metricValue != 0: publish
            } else {
                double percentChange = Math.abs((metricValue - prev) / prev) * 100.0;
                if (percentChange < PERCENTAGE_THRESHOLD) {
                    long lastPub = lastPublishedAt.getOrDefault(historyKey, 0L);
                    // If metric was never published before (lastPub == 0L), publish it now as initial value
                    if (lastPub == 0L) {
                        log.info(">>>>>>>>>>>>>>>>> AdaptiveMetricsFilter: Publishing metric {} for node {} as initial value: value={}, change={}% (first publish)", metricName, nodeAddress, metricValue, percentChange);
                        // Don't continue - fall through to publish
                    } else {
                        long elapsedSinceLastPub = timestamp - lastPub;
                        long thresholdSeconds = Duration.ofMinutes(30).toSeconds();  // Netdata timestamps are in seconds
                        if (elapsedSinceLastPub < thresholdSeconds) {
                            log.info(">>>>>>>>>>>>>>>>> AdaptiveMetricsFilter: Skipping metric {} for node {}: value={}, prev={}, change={}%, currentTimestamp={}, lastPublishedAt={}, elapsedSinceLastPub={}s (below 30min threshold)", metricName, nodeAddress, metricValue, prev, percentChange, timestamp, lastPub, elapsedSinceLastPub);
                            history.removeFirst();
                            history.addLast(new MetricSample(metricValue, timestamp));
                            return false;
                        }
                        // else: more than 30 minutes since last publish -> allow publish
                    }
                }
            }
            history.removeFirst();
        }
        history.addLast(new MetricSample(metricValue, timestamp));
        long prevLastPub = lastPublishedAt.getOrDefault(historyKey, 0L);
        log.info(">>>>>>>>>>>>>>>>> AdaptiveMetricsFilter: Can publish metric {} for node {}: value={}, prev={}, timestamp={}, lastPublishedAt={}", metricName, nodeAddress, metricValue, history.size() == 2 ? history.peekFirst().value : "N/A", timestamp, prevLastPub);

        return true;
    }

    @Override
    public void postPublishMetric(String metricName, double metricValue, long timestamp, String nodeAddress, Context context) {
        log.info(">>>>>>>>>>>>>>>>> AdaptiveMetricsFilter postPublishMetric: name={}, value={}, ts={}, nodeAddress={}, context={}", metricName, metricValue, timestamp, nodeAddress, context);
        String historyKey = metricName + "@" + nodeAddress;
        lastPublishedAt.put(historyKey, (Long)context.extra());
    }

    @Override
    public void notPublishedMetric(String metricName, double metricValue, long timestamp, String nodeAddress, Context context) {
        log.info(">>>>>>>>>>>>>>>>> AdaptiveMetricsFilter notPublishedMetric: name={}, value={}, ts={}, nodeAddress={}, context={}", metricName, metricValue, timestamp, nodeAddress, context);
    }
}
