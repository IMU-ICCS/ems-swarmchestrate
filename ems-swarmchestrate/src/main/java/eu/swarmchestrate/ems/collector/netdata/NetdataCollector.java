/*
 * Copyright (C) 2017-2026 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.swarmchestrate.ems.collector.netdata;

import gr.iccs.imu.ems.common.collector.AbstractEndpointCollector;
import gr.iccs.imu.ems.common.collector.CollectorContext;
import gr.iccs.imu.ems.common.collector.netdata.INetdataCollector;
import gr.iccs.imu.ems.common.collector.netdata.NetdataCollectorProperties;
import gr.iccs.imu.ems.util.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects measurements from Netdata http server
 */
@Slf4j
public class NetdataCollector extends AbstractEndpointCollector<HashMap> implements INetdataCollector {
    public final static String NETDATA_COLLECTION_START = "NETDATA_COLLECTION_START";
    public final static String NETDATA_COLLECTION_OK = "NETDATA_COLLECTION_OK";
    public final static String NETDATA_COLLECTION_ERROR = "NETDATA_COLLECTION_ERROR";
    public final static String NETDATA_CONN_OK = "NETDATA_CONN_OK";
    public final static String NETDATA_CONN_ERROR = "NETDATA_CONN_ERROR";
    public final static String NETDATA_NODE_OK = "NETDATA_NODE_OK";
    public final static String NETDATA_NODE_FAILED = "NETDATA_NODE_FAILED";

    protected NetdataCollectorProperties properties;
    protected RestClient restClient;

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

    @SuppressWarnings("unchecked")
    public NetdataCollector(String id, NetdataCollectorProperties properties, CollectorContext collectorContext, TaskScheduler taskScheduler, EventBus<String,Object,Object> eventBus) {
        super(id, properties, collectorContext, taskScheduler, eventBus);
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() {
        log.debug("Collectors::Netdata: properties: {}", properties);
        super.afterPropertiesSet();

        if (StringUtils.isBlank(properties.getUrl())) {
            String url = "http://127.0.0.1:19999/api/v1/allmetrics?format=json";
            log.debug("Collectors::Netdata: URL not specified. Assuming {}", url);
            properties.setUrl(url);
        }

        // Initialize REST client
        this.restClient = createRestClient();

        registerInternalEvents(NETDATA_COLLECTION_START, NETDATA_COLLECTION_OK, NETDATA_COLLECTION_ERROR,
                NETDATA_CONN_OK, NETDATA_CONN_ERROR, NETDATA_NODE_OK, NETDATA_NODE_FAILED);
    }

    protected ResponseEntity<HashMap> getData(String url) {
        return restClient.get().uri(url).retrieve().toEntity(HashMap.class);
    }

    protected void processData(HashMap data, String nodeAddress, ProcessingStats stats) {
        Map dataMap = data;
        for (Object key : dataMap.keySet()) {
            log.trace("Collectors::Netdata: ...Loop-1: key={}", key);
            if (key==null) continue;
            Map keyData = (Map)dataMap.get(key);
            log.trace("Collectors::Netdata: ...Loop-1: key-data={}", keyData);
            long timestamp = Long.parseLong( keyData.get("last_updated").toString() );
            Map dimensionsMap = (Map)keyData.get("dimensions");
            log.trace("Collectors::Netdata: ...Loop-1: ...dimensions-keys: {}", dimensionsMap.keySet());
            for (Object dimKey : dimensionsMap.keySet()) {
                log.trace("Collectors::Netdata: ...Loop-1: ...dimensions-key: {}", dimKey);
                if (dimKey==null) continue;
                String metricName = ("netdata."+ key + "."+ dimKey).replace(".", "__");
                log.trace("Collectors::Netdata: ...Loop-1: ...metric-name: {}", metricName);
                Map dimData = (Map)dimensionsMap.get(dimKey);
                Object valObj = dimData.get("value");
                log.trace("Collectors::Netdata: ...Loop-1: ...metric-value: {}", valObj);
                if (valObj!=null) {
                    double metricValue = Double.parseDouble(valObj.toString());
                    log.trace("Collectors::Netdata:           {} = {}", metricName, metricValue);
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
                                continue;
                            }
                            // prev == 0, metricValue != 0: publish
                        } else {
                            double percentChange = Math.abs((metricValue - prev) / prev) * 100.0;
                            if (percentChange < PERCENTAGE_THRESHOLD) {
                                long lastPub = lastPublishedAt.getOrDefault(historyKey, 0L);
                                // If metric was never published before (lastPub == 0L), publish it now as initial value
                                if (lastPub == 0L) {
                                    log.info("NetdataCollector: Publishing metric {} for node {} as initial value: value={}, change={}% (first publish)", metricName, nodeAddress, metricValue, percentChange);
                                    // Don't continue - fall through to publish
                                } else {
                                    long elapsedSinceLastPub = timestamp - lastPub;
                                    long thresholdSeconds = Duration.ofMinutes(30).toSeconds();  // Netdata timestamps are in seconds
                                    if (elapsedSinceLastPub < thresholdSeconds) {
                                        log.info("NetdataCollector: Skipping metric {} for node {}: value={}, prev={}, change={}%, currentTimestamp={}, lastPublishedAt={}, elapsedSinceLastPub={}s (below 30min threshold)", metricName, nodeAddress, metricValue, prev, percentChange, timestamp, lastPub, elapsedSinceLastPub);
                                        history.removeFirst();
                                        history.addLast(new MetricSample(metricValue, timestamp));
                                        continue;
                                    }
                                    // else: more than 30 minutes since last publish -> allow publish
                                }
                            }
                        }
                        history.removeFirst();
                    }
                    history.addLast(new MetricSample(metricValue, timestamp));
                    long prevLastPub = lastPublishedAt.getOrDefault(historyKey, 0L);
                    log.info("NetdataCollector: Publishing metric {} for node {}: value={}, prev={}, timestamp={}, lastPublishedAt={}", metricName, nodeAddress, metricValue, history.size() == 2 ? history.peekFirst().value : "N/A", timestamp, prevLastPub);
                    updateStats(publishMetricEvent(metricName, metricValue, timestamp, nodeAddress), stats);
                    lastPublishedAt.put(historyKey, timestamp);
                }
            }
            if (Thread.currentThread().isInterrupted()) break;
        }
    }
}