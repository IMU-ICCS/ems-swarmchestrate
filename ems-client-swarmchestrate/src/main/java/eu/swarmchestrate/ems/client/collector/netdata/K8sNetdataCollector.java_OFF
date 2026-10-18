/*
 * Copyright (C) 2017-2026 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.swarmchestrate.ems.client.collector.netdata;

import gr.iccs.imu.ems.api.EmsConstant;
import gr.iccs.imu.ems.baguette.client.IClientCollector;
import gr.iccs.imu.ems.baguette.client.collector.ClientCollectorContext;
import gr.iccs.imu.ems.brokercep.event.EventMap;
import gr.iccs.imu.ems.common.collector.CollectorConstant;
import gr.iccs.imu.ems.common.collector.CollectorContext;
import gr.iccs.imu.ems.common.collector.netdata.INetdataCollector;
//import gr.iccs.imu.ems.common.collector.netdata.NetdataCollectorProperties;
import gr.iccs.imu.ems.util.EventBus;
import gr.iccs.imu.ems.util.StrUtil;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collects measurements from Netdata agents in a Kubernetes cluster
 */
@Slf4j
@Component("K8sNetdataCollectorWithAdaptiveFiltering")
@RequiredArgsConstructor
public class K8sNetdataCollector implements IClientCollector, INetdataCollector, InitializingBean {

    public static final String K8S_NODE_ADDRESS_ENV_VAR = "NODE_ADDRESS";

    public enum RESULTS_AGGREGATION { NONE, SUM, AVERAGE, COUNT, MIN, MAX }

    @Data
    private static class ConfigContext {
        // Sensor settings and their defaults
        int port = 19999;
        int apiVer = 2;
        String urlSuffix = null;
        String context = null;
        String dimensions = "*";

        // Pod selection
        boolean isK8s;
        String namespace = "default";
//        String[] podPatternsInclude;
//        String[] podPatternsExclude;
//        boolean podPatternsIgnoreCase = true;

        // Kubernetes behaviour (i.e. when in a pod)
        String k8sNodeAddressEnvVar;

        // Non-kubernetes behaviour (i.e. when not in a pod)
        boolean enabledWhenNotInPod = true;
        boolean useAddressesFromCollectorContext = true;
        String whenNotInPodAddress = "localhost";

        // Post-processing settings
        String destination;
        boolean skipValueOnError = true;
        double valueOnError = Double.NEGATIVE_INFINITY;
        Collection<String> components;      // Pods
        RESULTS_AGGREGATION aggregation = RESULTS_AGGREGATION.NONE;
    }

    protected final static Set<String> SENSOR_CONFIG_KEYS_EXCLUDED =
            Set.of("endpoint", "port", "type", "_containerName", "results-aggregation", "components", "namespace");
    protected final static String NETDATA_DATA_API_V1_PATH = "/api/v1/data";
    protected final static String NETDATA_DATA_API_V2_PATH = "/api/v2/data";
    protected final static String DEFAULT_NETDATA_DATA_API_PATH = NETDATA_DATA_API_V2_PATH;

    private final NetdataCollectorProperties2 properties;
    private final CollectorContext collectorContext;
    private final TaskScheduler taskScheduler;
    private final EventBus<String, Object, Object> eventBus;
    private final RestClient restClient = RestClient.create();
    private final List<ScheduledFuture<?>> scheduledFuturesList = new LinkedList<>();
    private boolean started;
    private List<Map<String, Object>> configurations;


    // FIFO to keep last 2 values per metric/node (store value + timestamp)
    private final Map<String, Deque<MetricSample>> metricHistory = new ConcurrentHashMap<>();
    // track last time a metric was published (milliseconds since epoch)
    private final Map<String, Long> lastPublishedAt = new ConcurrentHashMap<>();
//    private final double PERCENTAGE_THRESHOLD = 10.0;

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
        log.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!  Initializing K8s Netdata Collector");
        if (!(collectorContext instanceof ClientCollectorContext))
            throw new IllegalArgumentException("Invalid CollectorContext provided. Expected: ClientCollectorContext, but got "+collectorContext.getClass().getName());
    }

    @Override
    public void setConfiguration(Object config) {
        if (config instanceof List sensorConfigList) {
            List<Map<String, Object>> newConfigurations = sensorConfigList.stream()
                    .filter(o -> o instanceof Map)
                    .filter(map -> ((Map) map).keySet().stream().allMatch(k -> k instanceof String))
                    .toList();
            log.debug("K8sNetdataCollector: setConfiguration: newConfigurations: {}", newConfigurations);

            // If configuration is set while collector is running we need to restart it
            if (started) {
                if ((configurations==null || configurations.isEmpty() && ! newConfigurations.isEmpty())) {
                    configurations = newConfigurations;
                    log.info("K8sNetdataCollector: setConfiguration: Collector configuration updated while collector is running");
                    log.debug("K8sNetdataCollector: setConfiguration: Restarting collector");
                    stop();
                    start();
                    log.info("K8sNetdataCollector: setConfiguration: Restarted collector");
                }
            }
            // If collector is not running update configuration anyway
            if (!started) {
                configurations = newConfigurations;
                log.info("K8sNetdataCollector: setConfiguration: Collector configuration updated");
            }
        } else
            log.warn("K8sNetdataCollector: setConfiguration: Ignoring unsupported configuration object: {}", config);
    }

    @Override
    public void start() {
        if (started) return;
        if (configurations!=null)
            doStart();
        started = true;
        log.debug("K8sNetdataCollector: Started");
    }

    @Override
    public void stop() {
        if (!started) return;
        started = false;
        doStop();
        log.debug("K8sNetdataCollector: Stopped");
    }

    private synchronized void doStart() {
        log.debug("K8sNetdataCollector: doStart(): BEGIN: configuration={}", configurations);
        log.trace("K8sNetdataCollector: doStart(): BEGIN: scheduledFuturesList={}", scheduledFuturesList);

        // Get Netdata agent address and port from env. vars
        /*String netdataAddress = null;
        if (StringUtils.isBlank(netdataAddress)) netdataAddress = System.getenv("NETDATA_ADDRESS");
        if (StringUtils.isBlank(netdataAddress)) netdataAddress = System.getenv("NETDATA_IP");
        if (StringUtils.isBlank(netdataAddress)) netdataAddress = System.getenv("HOST_IP");
        if (StringUtils.isBlank(netdataAddress)) netdataAddress = "127.0.0.1";
        log.trace("K8sNetdataCollector: doStart(): netdataAddress={}", netdataAddress);

        int netdataPort = Integer.parseInt(
                StringUtils.defaultIfBlank(System.getenv("NETDATA_PORT"), "19999").trim());
        final String baseUrl = String.format("http://%s:%d", netdataAddress.trim(), netdataPort);
        log.trace("K8sNetdataCollector: doStart(): baseUrl={}", baseUrl);*/

        // Process each sensor configuration
        AtomicInteger sensorNum = new AtomicInteger(0);
        configurations.forEach(map -> {
            log.debug("K8sNetdataCollector: doStart(): Sensor-{}: map={}", sensorNum.incrementAndGet(), map);

            // Check if it is a Pull sensor. (Push sensors are ignored)
            if ("true".equalsIgnoreCase( get(map, "pushSensor", "false") )) {
                log.debug("K8sNetdataCollector: doStart(): Sensor-{}: It is a Push sensor. Ignoring this sensor", sensorNum.get());
                return;
            }
            // else it is a Pull sensor

            // Get destination (topic) name
            String destinationName = get(map, "name", null);
            log.trace("K8sNetdataCollector: doStart(): Sensor-{}: destination={}", sensorNum.get(), destinationName);
            if (StringUtils.isBlank(destinationName)) {
                log.warn("K8sNetdataCollector: doStart(): Sensor-{}: No destination found in sensor config: {}", sensorNum.get(), map);
                return;
            }

            // Initialize configuration context
            ConfigContext cfgCtx = new ConfigContext();
            cfgCtx.destination = destinationName;

            // Build Netdata URL
            if (map.get("configuration") instanceof Map cfgMap) {
                log.trace("K8sNetdataCollector: doStart(): Sensor-{}: cfgMap={}", sensorNum.get(), cfgMap);

                // Get port
                try {
                    cfgCtx.port = Integer.parseInt(get(cfgMap, "port", "" + cfgCtx.port));
                    log.debug("K8sNetdataCollector: doStart(): Sensor-{}: Netdata agent port: {}", sensorNum.get(), cfgCtx.port);
                } catch (Exception e) {
                    log.warn("K8sNetdataCollector: doStart(): Sensor-{}: Invalid port specified in configuration: {}", sensorNum.get(), cfgMap);
                }

                // Get value aggregation
                try {
                    String s = get(cfgMap, "results-aggregation", cfgCtx.aggregation.name());
                    if (StringUtils.isNotBlank(s)) {
                        cfgCtx.aggregation = RESULTS_AGGREGATION.valueOf(s.trim().toUpperCase());
                    }
                    log.debug("K8sNetdataCollector: doStart(): Sensor-{}: Netdata results aggregation: {}", sensorNum.get(), cfgCtx.aggregation);
                } catch (Exception e) {
                    log.warn("K8sNetdataCollector: doStart(): Sensor-{}: Invalid results-aggregation specified in configuration: {}", sensorNum.get(), cfgMap);
                }

                // Get component name
                if (cfgMap.get("components") instanceof Collection)
                    cfgCtx.components = (Collection<String>) cfgMap.get("components");
                if (cfgMap.get("components") instanceof String s)
                    cfgCtx.components = Arrays.asList(s.trim().split("[, \\t]+"));
                log.trace("K8sNetdataCollector: doStart(): Sensor-{}: cfgCtx.components: {}", sensorNum.get(), cfgCtx.components);

                // Get K8S namespace (defaults to 'default')
                cfgCtx.namespace = cfgMap.getOrDefault("namespace", "default").toString();
                log.trace("K8sNetdataCollector: doStart(): Sensor-{}: namespace: {}", sensorNum.get(), cfgCtx.namespace);

                // Get pod include/exclude patterns
/*                cfgCtx.podPatternsIgnoreCase =
                        Boolean.parseBoolean(get(cfgMap, "pod-patterns-ignore-case", "true"));
                cfgCtx.podPatternsInclude =
                        Arrays.stream(get(cfgMap, "pod-patterns-include", "").split(","))
                                .filter(StringUtils::isNotBlank).map(String::trim)
                                .toList().toArray(new String[0]);
                cfgCtx.podPatternsExclude =
                        Arrays.stream(get(cfgMap, "pod-patterns-exclude", "").split(","))
                                .filter(StringUtils::isNotBlank).map(String::trim)
                                .toList().toArray(new String[0]);
                log.trace("K8sNetdataCollector: doStart(): Sensor-{}: pod-patterns: include={}, exclude={}, ignore-case={}",
                        sensorNum.get(), Arrays.asList(cfgCtx.podPatternsInclude),
                        Arrays.asList(cfgCtx.podPatternsExclude), cfgCtx.podPatternsIgnoreCase);
                if (cfgCtx.podPatternsInclude.length==0) cfgCtx.podPatternsInclude = null;
                if (cfgCtx.podPatternsExclude.length==0) cfgCtx.podPatternsExclude = null;*/

                // Process 'configuration' map entries, to build metric URL
                Map<String, Object> sensorConfig = (Map<String, Object>) cfgMap;
                String endpoint = get(sensorConfig, "endpoint", DEFAULT_NETDATA_DATA_API_PATH);
                log.trace("K8sNetdataCollector: doStart(): Sensor-{}: endpoint={}", sensorNum.get(), endpoint);

                if (NETDATA_DATA_API_V1_PATH.equalsIgnoreCase(endpoint)) {
                    cfgCtx.apiVer = 1;

                    // If expanded by a shorthand expression
                    cfgCtx.context = get(sensorConfig, CollectorConstant.NETDATA_METRIC_KEY, null);
                    if (StringUtils.isNotBlank(cfgCtx.context))
                        addEntryIfMissingOrBlank(sensorConfig, "context", cfgCtx.context);

                    // Else check sensor config for 'context' key
                    cfgCtx.context = get(sensorConfig, "context", null);

                    addEntryIfMissingOrBlank(sensorConfig, "dimensions", "*");
                    addEntryIfMissingOrBlank(sensorConfig, "after", "-1");
                    addEntryIfMissingOrBlank(sensorConfig, "group", "average");
                    addEntryIfMissingOrBlank(sensorConfig, "format", "json2");
                } else
                if (NETDATA_DATA_API_V2_PATH.equalsIgnoreCase(endpoint)) {
                    cfgCtx.apiVer = 2;

                    // If expanded by a shorthand expression
                    cfgCtx.context = get(sensorConfig, CollectorConstant.NETDATA_METRIC_KEY, null);
                    if (StringUtils.isNotBlank(cfgCtx.context))
                        addEntryIfMissingOrBlank(sensorConfig, "scope_contexts", cfgCtx.context);

                    // Else check sensor config for 'scope_contexts' or 'context' key
                    cfgCtx.context = get(sensorConfig, "scope_contexts", null);
                    if (StringUtils.isBlank(cfgCtx.context))
                        cfgCtx.context = get(sensorConfig, "context", null);

                    cfgCtx.isK8s = StringUtils.startsWithIgnoreCase(cfgCtx.context, "k8s");
                    if (cfgCtx.isK8s) {
                        addEntryIfMissingOrBlank(sensorConfig, "group_by", "label");
                        addEntryIfMissingOrBlank(sensorConfig, "group_by_label", "k8s_namespace,k8s_pod_name");
                    }
                    addEntryIfMissingOrBlank(sensorConfig, "dimensions", "*");
                    addEntryIfMissingOrBlank(sensorConfig, "after", "-1");
                    addEntryIfMissingOrBlank(sensorConfig, "time_group", "average");
                    addEntryIfMissingOrBlank(sensorConfig, "format", "ssv");
                } else {
                    log.warn("K8sNetdataCollector: doStart(): Sensor-{}: Invalid Netdata endpoint found in sensor config: {}", sensorNum.get(), map);
                    return;
                }
                cfgCtx.dimensions = get(sensorConfig, "dimensions", cfgCtx.dimensions);

                StringBuilder sb = new StringBuilder(endpoint);
                final AtomicBoolean first = new AtomicBoolean(true);
                sensorConfig.forEach((key, value) -> {
                    if (StringUtils.isNotBlank(key) && ! SENSOR_CONFIG_KEYS_EXCLUDED.contains(key)) {
                        if (value instanceof String valueStr) {
                            sb.append(first.get() ? "?" : "&").append(key).append("=").append(valueStr);
                            first.set(false);
                        }
                    }
                });

                if (StringUtils.isNotBlank(cfgCtx.context)) {
                    cfgCtx.urlSuffix = /*baseUrl +*/ sb.toString();
                } else {
                    log.warn("K8sNetdataCollector: doStart(): Sensor-{}: No 'context' found in sensor configuration: {}", sensorNum.get(), map);
                    return;
                }
            } else {
                log.warn("K8sNetdataCollector: doStart(): Sensor-{}: No sensor configuration found is spec: {}", sensorNum.get(), map);
                return;
            }
            log.trace("K8sNetdataCollector: doStart(): Sensor-{}: Metric urlSuffix={}", sensorNum.get(), cfgCtx.urlSuffix);

            // Get interval and configure scheduler
            long period = 60;
            TimeUnit unit = TimeUnit.SECONDS;
            Duration duration = null;
            if (map.get("interval") instanceof Map intervalMap) {
                log.trace("K8sNetdataCollector: doStart(): Sensor-{}: intervalMap={}", sensorNum.get(), intervalMap);
                period = Long.parseLong(get(intervalMap, "period", Long.toString(period)));
                if (period>0) {
                    String unitStr = get(intervalMap, "unit", unit.name());
                    unit = StringUtils.isNotBlank(unitStr)
                            ? TimeUnit.valueOf(unitStr.toUpperCase().trim()) : TimeUnit.SECONDS;
                    duration = Duration.of(period, unit.toChronoUnit());
                }
            }
            log.trace("K8sNetdataCollector: doStart(): Sensor-{}: duration-from-spec={}", sensorNum.get(), duration);
            if (duration==null) {
                duration = Duration.of(period, unit.toChronoUnit());
            }
            log.trace("K8sNetdataCollector: doStart(): Sensor-{}: duration={}", sensorNum.get(), duration);

            // Get settings for kubernetes behavior
            cfgCtx.k8sNodeAddressEnvVar =
                    get(cfgMap, "k8sNodeAddressEnvVar", cfgCtx.k8sNodeAddressEnvVar);

            // Get settings for non-kubernetes behavior
            cfgCtx.enabledWhenNotInPod = "true".equalsIgnoreCase(
                    get(cfgMap, "enabledWhenNotInPod", Boolean.toString(cfgCtx.enabledWhenNotInPod)) );
            cfgCtx.useAddressesFromCollectorContext = "true".equalsIgnoreCase(
                    get(cfgMap, "useAddressesFromCollectorContext", Boolean.toString(cfgCtx.useAddressesFromCollectorContext)) );
            cfgCtx.whenNotInPodAddress =
                    get(cfgMap, "whenNotInPodAddress", cfgCtx.whenNotInPodAddress);

            // Print context
            log.debug("K8sNetdataCollector: doStart(): cfgCtx: {}", cfgCtx);

            // Schedule data collection task
            scheduledFuturesList.add( taskScheduler.scheduleAtFixedRate(() -> {
                log.debug("K8sNetdataCollector: Sensor-{}: Starting collection...\n{}", sensorNum.get(), cfgCtx);
                collectData(cfgCtx);
                log.debug("K8sNetdataCollector: Sensor-{}: Starting collection...Done", sensorNum.get());
            }, duration) );
            log.debug("K8sNetdataCollector: doStart(): Sensor-{}: destination={}, components={}, interval={}, urlSuffix={}",
                    sensorNum.get(), cfgCtx.destination, cfgCtx.components, duration, cfgCtx.urlSuffix);
            log.info("K8sNetdataCollector: Collecting Netdata metric '{}.{}' into '{}', every {} {}",
                    cfgCtx.context, cfgCtx.dimensions, cfgCtx.destination, period, unit.name().toLowerCase());
        });
        log.trace("K8sNetdataCollector: doStart(): scheduledFuturesList={}", scheduledFuturesList);
        log.debug("K8sNetdataCollector: doStart(): END");
    }

    private String get(Map<String,Object> map, String key, String defaultValue) {
        Object valObj;
        if (!map.containsKey(key) || (valObj = map.get(key))==null) return defaultValue;
        String value = valObj.toString();
        if (StringUtils.isBlank(value)) return defaultValue;
        return value.trim();
    }

    private void addEntryIfMissingOrBlank(Map<String, Object> map, String key, Object value) {
        if (map.containsKey(key) && map.get(key)!=null)
            if (map.get(key) instanceof String s && StringUtils.isNotBlank(s))
                return;
        map.put(key, value);
    }

    private void collectData(ConfigContext cfgCtx) {
        long startTm = System.currentTimeMillis();
        log.debug("K8sNetdataCollector: collectData(): BEGIN: apiVer={}, urlSuffix={}, port={}, results-aggregation={}, destination={}, components={}",
                cfgCtx.apiVer, cfgCtx.urlSuffix, cfgCtx.port, cfgCtx.aggregation, cfgCtx.destination, cfgCtx.components);

        // Get nodes to scrape
        // (a) First check for the K8S node address
        Set<Serializable> nodesToScrape = Set.of();
        String address = System.getenv( StringUtils.firstNonBlank(
                cfgCtx.k8sNodeAddressEnvVar, properties.getK8sNodeAddressEnvVar(), K8S_NODE_ADDRESS_ENV_VAR) );
        if (StringUtils.isNotBlank(address)) {
            log.debug("K8sNetdataCollector: collectData(): In-Pod: Netdata node-to-scrape={}", address);
            nodesToScrape = Set.of(address);
        } else
        if (cfgCtx.isEnabledWhenNotInPod()) {
            // (b) Next check for the 'whenNotInPodAddress' (localhost by default)
            address = cfgCtx.getWhenNotInPodAddress();
            log.debug("K8sNetdataCollector: collectData(): Not-In-Pod: Netdata node-to-scrape={}", address);
            if (StringUtils.isNotBlank(address)) {
                nodesToScrape = Set.of(address);
            }
        }
        log.debug("K8sNetdataCollector: collectData(): Netdata node-to-scrape={}", nodesToScrape);

        // (c) If an Aggregator, collect data from nodes without client
        if (cfgCtx.useAddressesFromCollectorContext) {
            log.trace("K8sNetdataCollector: collectData(): Nodes without clients in Zone: {}", collectorContext.getNodesWithoutClient());
            log.trace("K8sNetdataCollector: collectData(): Is Aggregator: {}", collectorContext.isAggregator());
            if (collectorContext.isAggregator()) {
                if (collectorContext.getNodesWithoutClient()!=null && ! collectorContext.getNodesWithoutClient().isEmpty()) {
                    log.debug("K8sNetdataCollector: collectData(): Collecting metrics from remote nodes (without EMS client): {}",
                            collectorContext.getNodesWithoutClient());
                    nodesToScrape = new LinkedHashSet<>(nodesToScrape);
                    nodesToScrape.addAll(collectorContext.getNodesWithoutClient());
                } else
                    log.debug("K8sNetdataCollector: collectData(): No remote nodes (without EMS client)");
            }
            log.debug("K8sNetdataCollector: collectData(): Netdata updated nodes-to-scrape={}", nodesToScrape);
        }

        if (nodesToScrape.isEmpty()) {
            long endTm = System.currentTimeMillis();
            log.debug("K8sNetdataCollector: collectData(): END: No Netdata node to scrape: duration={}ms", endTm - startTm);
            log.warn("K8sNetdataCollector: collectData(): No Netdata nodes to scrape  (enabledWhenNotInPod={}, useAddressesFromCollectorContext={}, isAggregator={})",
                    cfgCtx.enabledWhenNotInPod, cfgCtx.useAddressesFromCollectorContext, collectorContext.isAggregator());
            return;
        }

        // Scrape Netdata node(s)
        nodesToScrape.forEach(nodeAddress -> {
            String url = "http://%s:%d%s".formatted(nodeAddress, cfgCtx.port, cfgCtx.urlSuffix);
            try {
                log.info("K8sNetdataCollector: collectData(): Scraping Netdata node: {}", url);
                collectDataFromNode(cfgCtx, url, nodeAddress.toString());
            } catch (Exception e) {
                log.warn("K8sNetdataCollector: collectData(): EXCEPTION while Scraping Netdata node: {} -- Exception: {}", url, StrUtil.exceptionToDetailsString(e, true));
                log.debug("K8sNetdataCollector: collectData(): EXCEPTION while Scraping Netdata node: {}\n", url, e);
            }
        });

        long endTm = System.currentTimeMillis();
        log.debug("K8sNetdataCollector: collectData(): END: duration={}ms", endTm-startTm);
    }

    private void collectDataFromNode(ConfigContext cfgCtx, String url, String address) {
        long startTm = System.currentTimeMillis();
        log.debug("K8sNetdataCollector: collectDataFromNode(): BEGIN: apiVer={}, url={}, results-aggregation={}, destination={}, components={}, node-address={}",
                cfgCtx.apiVer, url, cfgCtx.aggregation, cfgCtx.destination, cfgCtx.components, address);

        Map<String,Double> resultsMap = new HashMap<>();
        long timestamp = -1L;
        if (cfgCtx.apiVer==1) {
            Map response = restClient.get()
                    .uri(url)
                    .retrieve()
                    .body(Map.class);
            // Need to call for each pod separately
            // or get the total
            timestamp = Long.parseLong( response.get("before").toString() );
            List<String> chart_ids = (List) response.get("chart_ids");
            List<String> dimension_ids = (List) response.get("dimension_ids");
            List<String> latest_values = (List) response.get("view_latest_value");
            for (int i=0, n=chart_ids.size(); i<n; i++) {
                String id = chart_ids.get(i) + "|" + dimension_ids.get(i);
                try {
                    if (includeResult(cfgCtx, id)) {
                        double v = Double.parseDouble(latest_values.get(i));
                        resultsMap.put(id, v);
                    }
                } catch (Exception e) {
                    log.warn("K8sNetdataCollector: collectDataFromNode(): ERROR at index #{}: id={}, value={}, Exception: ",
                            i, id, latest_values.get(i), e);
                    if (! cfgCtx.skipValueOnError)
                        resultsMap.put(id, cfgCtx.valueOnError);
                }
            }
        } else
        if (cfgCtx.apiVer==2) {
            log.debug("K8sNetdataCollector: collectDataFromNode(): Calling Netdata: apiVer={}, url={}", cfgCtx.apiVer, url);
            Map response = restClient.get()
                    .uri(url)
                    .retrieve()
                    .body(Map.class);
            log.trace("K8sNetdataCollector: collectDataFromNode(): apiVer={}, response={}", cfgCtx.apiVer, response);

            if (response!=null) {
                Map view = (Map) response.get("view");

                // Check and (debug) print few values
                try {
                    String result = Objects.requireNonNullElse(response.get("result"), "").toString();
                    //resultsMap.put("result", result);
                    if (view!=null) {
                        long after = Long.parseLong(view.get("after").toString());
                        long before = Long.parseLong(view.get("before").toString());
                        timestamp = before;
                        log.trace("K8sNetdataCollector: collectDataFromNode(): result={}, after={}, before={}", result, after, before);
                    }
                } catch (Exception e) {
                    Map v = (Map) response.get("view");
                    if (log.isDebugEnabled())
                        log.debug("K8sNetdataCollector: collectDataFromNode(): Problem with acquired values: result={}, view={}, after={}, before={} :: Exception: ",
                                response.get("result"), v, v!=null ? v.get("after") : null, v!=null ? v.get("before") : null, e);
                    else
                        log.warn("K8sNetdataCollector: collectDataFromNode(): Problem with acquired values: result={}, view={}, after={}, before={}",
                                response.get("result"), v, v!=null ? v.get("after") : null, v!=null ? v.get("before") : null);
                }

                // Extract measurements
                if (view!=null && view.get("dimensions") instanceof Map) {
                    Map dimensions = (Map) view.get("dimensions");
                    List<String> ids = (List<String>) dimensions.get("ids");
                    List<String> units = (List<String>) dimensions.get("units");
                    Map stsMap = (Map) dimensions.get("sts");
                    List<Number> values = stsMap==null ? null : (List<Number>) stsMap.get("avg");
                    log.trace("K8sNetdataCollector: collectDataFromNode():    ids={}", ids);
                    log.trace("K8sNetdataCollector: collectDataFromNode():  units={}", units);
                    log.trace("K8sNetdataCollector: collectDataFromNode(): values={}", values);
                    if (ids!=null && units!=null && values!=null) {
                        for (int i = 0, n = ids.size(); i < n; i++) {
                            try {
                                if (includeResult(cfgCtx, ids.get(i))) {
                                    double v = values.get(i).doubleValue();
                                    resultsMap.put(ids.get(i), v);
                                }
                            } catch (Exception e) {
                                log.warn("K8sNetdataCollector: collectDataFromNode(): ERROR at index #{}: id={}, value={}, Exception: ",
                                        i, ids.get(i), values.get(i), e);
                                if (!cfgCtx.skipValueOnError)
                                    resultsMap.put(ids.get(i), cfgCtx.valueOnError);
                            }
                        }
                    }
                }
            }
        }

        log.debug("K8sNetdataCollector: collectDataFromNode(): Data collected: timestamp={}, results={}", timestamp, resultsMap);

        // Publish collected data to destination
        if (!resultsMap.isEmpty()) {
            final long timestamp1 = timestamp;
            Map<String, CollectorContext.PUBLISH_RESULT> publishResults = new LinkedHashMap<>();
            if (cfgCtx.aggregation == RESULTS_AGGREGATION.NONE) {
                resultsMap.forEach((k, v) -> {
                    publishResults.put(k + "=" + v, publishMetricEvent(cfgCtx.destination, k, v, timestamp1, address));
                });
            } else {
                double result = switch (cfgCtx.aggregation) {
                    case SUM -> resultsMap.values().stream().mapToDouble(Double::doubleValue).sum();
                    case AVERAGE -> resultsMap.values().stream().mapToDouble(Double::doubleValue).average().orElse(0);
                    case COUNT -> resultsMap.values().size();
                    case MIN -> resultsMap.values().stream().mapToDouble(Double::doubleValue).min().orElse(0);
                    case MAX -> resultsMap.values().stream().mapToDouble(Double::doubleValue).max().orElse(0);
                    case NONE -> throw new IllegalArgumentException("FATAL: Execution should never have reached this point");
                };
                if (!resultsMap.isEmpty())
                    publishResults.put(null, publishMetricEvent(cfgCtx.destination, null, result, timestamp1, address));
                //else log.trace("K8sNetdataCollector: collectDataFromNode(): No results. Will not publish anything");
            }
            log.debug("K8sNetdataCollector: collectDataFromNode(): Events published: results={}", publishResults);
        } else {
            log.debug("K8sNetdataCollector: collectDataFromNode(): No Events published because resultsMap is empty");
        }

        long endTm = System.currentTimeMillis();
        log.debug("K8sNetdataCollector: collectDataFromNode(): END: duration={}ms", endTm-startTm);
    }

    private boolean includeResult(ConfigContext cfgCtx, String id) {
        log.debug("K8sNetdataCollector: includeResult(): BEGIN: components={}, id={}",
                cfgCtx.components, id);

        if (! cfgCtx.isK8s) {
            log.debug("K8sNetdataCollector: includeResult(): END: Not K8S metric: result={}", true);
            return true;
        }

        // Get applicable namespace and set prefix
        String nsPrefix = "";
        if (StringUtils.isNotBlank(cfgCtx.namespace)) {
            nsPrefix = cfgCtx.namespace+",";
        }

        // Check if applicable components are provided
        if (cfgCtx.components==null || cfgCtx.components.isEmpty()
                || cfgCtx.components.size()==1 && StringUtils.isBlank(cfgCtx.components.iterator().next()))
        {
            // No applicable components specified. Assuming all pods are accepted
            if (nsPrefix.isEmpty()) {
                // No namespace specified. Assuming all pods of all namespaces are accepted
                log.debug("K8sNetdataCollector: includeResult(): END: pods=ALL, ns=ALL: result={}", true);
                return true;
            } else {
                boolean result = StringUtils.startsWith(id, nsPrefix);
                log.debug("K8sNetdataCollector: includeResult(): END: pods=ALL, ns={}: result={}", cfgCtx.namespace, result);
                return result;
            }
        }

        // Applicable components specified. For each one...
        for (String componentName : cfgCtx.components) {
            log.trace("K8sNetdataCollector: includeResult():   component={}", componentName);

            boolean matchDeployment = false;
            boolean matchDaemonSet = false;
            String prefix = nsPrefix + componentName;
            log.trace("K8sNetdataCollector: includeResult():     prefix={}", prefix);
            if (StringUtils.startsWith(id, prefix)) {
                String remainder = id.substring(prefix.length());
                log.trace("K8sNetdataCollector: includeResult():       remainder={}", remainder);
                if (remainder.charAt(0)=='-') {
                    log.trace("K8sNetdataCollector: includeResult():       remainder: size={}, char-11={}", remainder.length(), remainder.charAt(remainder.length()-5-1));
                    int lastDashPos = remainder.length()-5-1;   // '5' is the size of POD UUID suffix
                    if (remainder.length()>=16 && remainder.charAt(lastDashPos)=='-') {
                        log.trace("K8sNetdataCollector: includeResult():       matching Deployment={}", remainder);
                        matchDeployment = true;
                    } else
                    if (remainder.length()==6) {
                        log.trace("K8sNetdataCollector: includeResult():       matching DaemonSet={}", remainder);
                        matchDaemonSet = true;
                    }
                }
            }
            log.trace("K8sNetdataCollector: includeResult():     matchDeployment={}, matchDaemonSet={}", matchDeployment, matchDaemonSet);

            /*if (cfgCtx.podPatternsInclude!=null)
                return cfgCtx.podPatternsIgnoreCase
                        ? StringUtils.containsAnyIgnoreCase(id, cfgCtx.podPatternsInclude)
                        : StringUtils.containsAny(id, cfgCtx.podPatternsInclude);
            else if (cfgCtx.podPatternsExclude!=null)
                return ! (cfgCtx.podPatternsIgnoreCase
                        ? StringUtils.containsAnyIgnoreCase(id, cfgCtx.podPatternsExclude)
                        : StringUtils.containsAny(id, cfgCtx.podPatternsExclude) );*/

            boolean result = matchDeployment || matchDaemonSet;
            if (result) {
                // Applicable component matched the 'id'
                log.debug("K8sNetdataCollector: includeResult(): END: pod={}, ns={}, result={}", componentName, cfgCtx.namespace, result);
                return result;
            }
        }

        // No applicable component name matched the 'id'
        boolean result = false;
        log.debug("K8sNetdataCollector: includeResult(): END: pods={}, ns={}: result={}", cfgCtx.components, cfgCtx.namespace, result);
        return result;
    }

    /*private boolean isUuid(String s, int start, int end) {
        for (int i=start; i<=end; i++) {
            char c = s.charAt(i);
            if (!('0' <= c && c <= '9' || 'a' <= c && c <= 'z')) return false;
        }
        return true;
    }*/

    private synchronized void doStop() {
        if (! log.isTraceEnabled())
            log.debug("K8sNetdataCollector: doStop():  BEGIN");
        else
            log.trace("K8sNetdataCollector: doStop():  BEGIN: scheduledFuturesList={}", scheduledFuturesList);
        // Cancel all task scheduler futures
        scheduledFuturesList.forEach(future -> future.cancel(true));
        scheduledFuturesList.clear();
        log.debug("K8sNetdataCollector: doStop():  END");
    }

    public synchronized void activeGroupingChanged(String oldGrouping, String newGrouping) {
        log.debug("K8sNetdataCollector: activeGroupingChanged: {} --> {}", oldGrouping, newGrouping);
    }

    protected CollectorContext.PUBLISH_RESULT publishMetricEvent(String metricName, String key, double metricValue, long timestamp, String nodeAddress) {

        // Adaptive Filtering -- BEGIN
        String historyKey = metricName + "@" + nodeAddress;
        metricHistory.putIfAbsent(historyKey, new ArrayDeque<>(2));
        Deque<MetricSample> history = metricHistory.get(historyKey);
        if (history.size() == 2) {
            MetricSample prevSample = history.peekFirst();
            double prev = prevSample.value;
            if (prev == 0) {
                if (metricValue == 0) {
                    log.debug("K8sNetdataCollector: Previous value is 0 and current value is also 0 for metric {} on node {}. Skipping publish. (timestamp={})", metricName, nodeAddress, timestamp);
                    history.removeFirst();
                    history.addLast(new MetricSample(metricValue, timestamp));
                    return CollectorContext.PUBLISH_RESULT.SKIPPED;
                }
                // prev == 0, metricValue != 0: publish
            } else {
                double percentChange = Math.abs((metricValue - prev) / prev) * 100.0;
                if (percentChange < properties.getPercentageThreshold()) {
                    long lastPub = lastPublishedAt.getOrDefault(historyKey, 0L);
                    // If metric was never published before (lastPub == 0L), publish it now as initial value
                    if (lastPub == 0L) {
                        log.debug("K8sNetdataCollector: Publishing metric {} for node {} as initial value: value={}, change={}% (first publish)", metricName, nodeAddress, metricValue, percentChange);
                        // Don't continue - fall through to publish
                    } else {
                        long elapsedSinceLastPub = timestamp - lastPub;
                        long thresholdSeconds = Duration.ofMinutes(30).toSeconds();  // Netdata timestamps are in seconds
                        if (elapsedSinceLastPub < thresholdSeconds) {
                            log.debug("K8sNetdataCollector: Skipping metric {} for node {}: value={}, prev={}, change={}%, currentTimestamp={}, lastPublishedAt={}, elapsedSinceLastPub={}s (below 30min threshold)", metricName, nodeAddress, metricValue, prev, percentChange, timestamp, lastPub, elapsedSinceLastPub);
                            history.removeFirst();
                            history.addLast(new MetricSample(metricValue, timestamp));
                            return CollectorContext.PUBLISH_RESULT.SKIPPED;
                        }
                        // else: more than 30 minutes since last publish -> allow publish
                    }
                }
            }
            history.removeFirst();
        }
        history.addLast(new MetricSample(metricValue, timestamp));
        long prevLastPub = lastPublishedAt.getOrDefault(historyKey, 0L);
        log.debug("K8sNetdataCollector: Publishing metric {} for node {}: value={}, prev={}, timestamp={}, lastPublishedAt={}", metricName, nodeAddress, metricValue, history.size() == 2 ? history.peekFirst().value : "N/A", timestamp, prevLastPub);
        lastPublishedAt.put(historyKey, timestamp);
        // Adaptive Filtering -- END

        EventMap event = new EventMap(metricValue, 1, timestamp);
        return sendEvent(metricName, metricName, key, event, null, true);
    }

    private CollectorContext.PUBLISH_RESULT sendEvent(String metricName, String originalTopic, String key, EventMap event, String nodeAddress, boolean createDestination) {
        event.setEventProperty(EmsConstant.EVENT_PROPERTY_SOURCE_ADDRESS, nodeAddress);
        event.getEventProperties().put(EmsConstant.EVENT_PROPERTY_EFFECTIVE_DESTINATION, metricName);
        event.getEventProperties().put(EmsConstant.EVENT_PROPERTY_ORIGINAL_DESTINATION, originalTopic);
        if (key!=null) event.getEventProperties().put(EmsConstant.EVENT_PROPERTY_DESTINATION_KEY, key);
        log.debug("K8sNetdataCollector:    Publishing metric: {}: {}", metricName, event.getMetricValue());
        CollectorContext.PUBLISH_RESULT result = collectorContext.sendEvent(null, metricName, event, createDestination);
        log.trace("K8sNetdataCollector:    Publishing metric: {}: {} -> result: {}", metricName, event.getMetricValue(), result);
        return result;
    }
}
