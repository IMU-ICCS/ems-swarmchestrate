/*
 * Copyright (C) 2017-2027 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0, unless
 * Esper library is used, in which case it is subject to the terms of General Public License v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import eu.nebulous.ems.translate.NameNormalization;
import eu.nebulous.ems.translate.NebulousEmsTranslatorProperties;
import gr.iccs.imu.ems.control.plugin.PreTranslationPlugin;
import gr.iccs.imu.ems.control.util.TopicBeacon;
import gr.iccs.imu.ems.translate.TranslationContext;
import gr.iccs.imu.ems.translate.dag.DAGNode;
import gr.iccs.imu.ems.translate.model.*;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ToscaToNebulousMetricModelPreTranslationPlugin implements PreTranslationPlugin {

    private final NebulousEmsTranslatorProperties properties;

    @Override
    public String preprocessModel(String toscaModelFile, String applicationId, Map<String,Object> additionalArguments) {
        String nebMetricModelFile = "NEB-"+toscaModelFile;
        log.warn("""
                 >>>>>>>>>>>>>>>>>>>>>>>>>  ToscaToNebulousMetricModelPreTranslationPlugin:
                             toscaModelFile: {}
                         nebMetricModelFile: {}
                              applicationId: {}
                        additionalArguments: {}
                 """,
                 toscaModelFile, nebMetricModelFile, applicationId, additionalArguments);

        tosca2nebulousMetricModel(toscaModelFile, nebMetricModelFile);

        return nebMetricModelFile;
    }

    protected void tosca2nebulousMetricModel(String inputFilePath, String outputFilePath) {
        try {
            log.info("Initializing YAML parser");
            // Initialize YAML parser
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

            File inputFile = Paths.get(properties.getModelsDir(), inputFilePath).toFile();
            log.info("Parsing TOSCA YAML from file: {}", inputFilePath);

            // Parse TOSCA YAML
            Map<String, Object> toscaYaml = yamlMapper.readValue(inputFile, Map.class);
            log.debug("Parsed TOSCA YAML: {}", toscaYaml);

            log.info("Translating TOSCA to CAML");
            // Translate TOSCA to CAML
            Map<String, Object> camlYaml = translateToscaToCaml(toscaYaml, inputFile);
            log.debug("Translated CAML YAML: {}", camlYaml);

            File outputFile = Paths.get(properties.getModelsDir(), outputFilePath).toFile();
            log.info("Writing translated CAML YAML to output file: {}", outputFilePath);
            // Write the translated CAML YAML to output file
            yamlMapper.writeValue(outputFile, camlYaml);

            log.info("Translation successful! Output written to: {}", outputFilePath);
        } catch (IOException e) {
            log.error("Error processing TOSCA model: {}", e);
            throw new RuntimeException(e);
        }
    }

    protected Map<String, Object> translateToscaToCaml(Map<String, Object> toscaYaml, File inputFile) {
        // Dispatch between TOSCA2, v1 parser and legacy parser
        Object toscaVersion = toscaYaml.get("tosca_definitions_version");
        if (toscaVersion != null && String.valueOf(toscaVersion).contains("tosca_2")) {
            log.info("Using TOSCA2 parser for TOSCA -> CAML translation");
            return translateToscaToCamlTOSCA2(toscaYaml, inputFile);
        }

        Map<String, Object> metadata = (Map<String, Object>) toscaYaml.get("metadata");
        Object versionObj = (metadata == null) ? null : metadata.get("version");
        String version = (versionObj == null) ? null : String.valueOf(versionObj);
        log.info("Detected TOSCA metadata.version: {}", version);

        if ("1.0".equals(version)) {
            log.info("Using v1 parser for TOSCA -> CAML translation");
            return translateToscaToCamlV1(toscaYaml, inputFile);
        } else {
            log.info("Using legacy parser for TOSCA -> CAML translation");
            return translateToscaToCamlLegacy(toscaYaml, inputFile);
        }
    }

    // New parser for metadata.version == 1.0 — respects `sensor`, `metric` and omits null busy-status
    @SuppressWarnings("unchecked")
    protected Map<String, Object> translateToscaToCamlV1(Map<String, Object> toscaYaml, File inputFile) {
        Map<String, Object> camlYaml = new LinkedHashMap<>();
        camlYaml.put("apiVersion", "nebulous/v1");
        camlYaml.put("kind", "MetricModel");
        log.info("Extracting components (v1)");
        List<Map<String, Object>> componentsCaml = new ArrayList<>();

        // Metadata
        Map<String, Object> metadata = (Map<String, Object>) toscaYaml.get("metadata");
        if (metadata == null) {
            metadata = new LinkedHashMap<>();
        }
        metadata.put("fileName", inputFile.getName());
        camlYaml.put("metadata", metadata);

        Map<String, Object> componentsData = (Map<String, Object>) toscaYaml.get("node_types");
        List<String> componentNames = new ArrayList<>();

        if (componentsData != null) {
            for (Map.Entry<String, Object> entry : componentsData.entrySet()) {
                Map<String, Object> componentCaml = new LinkedHashMap<>();
                String componentName = entry.getKey();
                componentCaml.put("name", componentName);
                componentNames.add(componentName);

                Map<String, Object> componentNodeTypes = (Map<String, Object>) entry.getValue();
                List<Map<String, Object>> metrics = new ArrayList<>();
                List<Map<String, Object>> requirements = new ArrayList<>();

                for (Map.Entry<String, Object> nodeType : componentNodeTypes.entrySet()) {
                    Map<String, Object> nodeData = (Map<String, Object>) nodeType.getValue();
                    List<Map<String, Object>> capabilities = (List<Map<String, Object>>) nodeData.get("capabilities");

                    if (capabilities != null) {
                        for (Map<String, Object> capability : capabilities) {
                            capability = (Map<String, Object>) capability.entrySet().iterator().next().getValue();

                            if ("capabilities.MetricMonitoringCapability".equals(capability.get("type"))) {
                                Map<String, Object> monitoringProperties = (Map<String, Object>) capability.get("properties");
                                if (monitoringProperties != null) {
                                    List<Map<String, Object>> raw = (List<Map<String, Object>>) monitoringProperties.get("raw");
                                    if (raw != null) {
                                        for (Map<String, Object> rawMetric : raw) {
                                            Map.Entry<String, Object> rawMetricEntry = (Map.Entry<String, Object>) rawMetric.entrySet().iterator().next();
                                            Map<String, Object> metric = new LinkedHashMap<>();
                                            metric.put("name", rawMetricEntry.getKey());

                                            Map<String, Object> rawMetricData = (Map<String, Object>) rawMetricEntry.getValue();
                                            if (rawMetricData != null) {
                                                Map<String, Object> sensor = new LinkedHashMap<>();
                                                // new format uses 'sensor' field for type
                                                sensor.put("type", rawMetricData.get("sensor"));

                                                // defensive copy of config
                                                Map<String, Object> sensorConfig = rawMetricData.get("config") != null
                                                        ? new LinkedHashMap<>((Map<String, Object>) rawMetricData.get("config"))
                                                        : new LinkedHashMap<>();

                                                // prefer explicit metric in config, otherwise fallback to top-level metric or collector_inst
                                                if (!sensorConfig.containsKey("metric")) {
                                                    Object metricVal = rawMetricData.get("metric");
                                                    if (metricVal == null) {
                                                        Object collectorInst = rawMetricData.get("collector_inst");
                                                        if (collectorInst != null) {
                                                            sensorConfig.put("metric", collectorInst);
                                                        }
                                                    } else {
                                                        sensorConfig.put("metric", metricVal);
                                                    }
                                                }

                                                // busy-status should be included only when present
                                                Object busyStatus = rawMetricData.get("busy-status");
                                                if (busyStatus != null) {
                                                    sensorConfig.put("busy-status", busyStatus);
                                                }

                                                sensor.put("config", sensorConfig);
                                                metric.put("sensor", sensor);

                                                String collectionOutput = (String) rawMetricData.get("collection_output");
                                                String collectionFrequency = (String) rawMetricData.get("collection_frequency");
                                                if (collectionOutput != null || collectionFrequency != null) {
                                                    metric.put("output", (collectionOutput == null ? "" : collectionOutput) + " " + (collectionFrequency == null ? "" : collectionFrequency));
                                                }
                                            }

                                            metrics.add(metric);
                                        }
                                    }

                                    List<Map<String, Object>> composite = (List<Map<String, Object>>) monitoringProperties.get("composite");
                                    if (composite != null) {
                                        for (Map<String, Object> compositeMetric : composite) {
                                            Map.Entry<String, Object> compositeMetricEntry = (Map.Entry<String, Object>) compositeMetric.entrySet().iterator().next();
                                            Map<String, Object> metric = new LinkedHashMap<>();
                                            metric.put("name", compositeMetricEntry.getKey());
                                            Map<String, Object> compositeData = (Map<String, Object>) compositeMetricEntry.getValue();

                                            if (compositeData != null) {
                                                Map<String, Object> formula = (Map<String, Object>) compositeData.get("formula");
                                                if (formula != null) {
                                                    String collectionOutput = (String) formula.get("collection_output");
                                                    String collectionFrequency = (String) formula.get("collection_frequency");
                                                    metric.put("formula", formula.get("type") + "(" + formula.get("argument") + ")");
                                                    metric.put("output", collectionOutput + " " + collectionFrequency);
                                                }

                                                Map<String, Object> window = (Map<String, Object>) compositeData.get("window");
                                                if (window != null) {
                                                    String windowType = (String) window.get("type");
                                                    String windowSize = (String) window.get("size");
                                                    metric.put("window", windowType + " " + windowSize);
                                                }

                                                Map<String, Object> processing = (Map<String, Object>) compositeData.get("processing");
                                                if (processing != null) {
                                                    String processingType = (String) processing.get("type");
                                                    String processingCriteria = (String) processing.get("criteria");
                                                    metric.put(processingType, processingCriteria);
                                                }

                                                metrics.add(metric);
                                            }
                                        }
                                    }
                                }
                            }

                            if ("capabilities.SloMonitoringCapability".equals(capability.get("type"))) {
                                List<Map<String, Object>> sloProperties = (List<Map<String, Object>>) capability.get("properties");
                                if (sloProperties != null) {
                                    for (Map<String, Object> slo : sloProperties) {
                                        Map.Entry<String, Object> sloEntry = (Map.Entry<String, Object>) slo.entrySet().iterator().next();
                                        Map<String, Object> sloRequirement = new LinkedHashMap<>();
                                        sloRequirement.put("name", sloEntry.getKey());
                                        sloRequirement.put("type", "slo");
                                        Map<String, Object> sloData = (Map<String, Object>) sloEntry.getValue();
                                        sloRequirement.put("constraint", sloData.get("constraint"));
                                        requirements.add(sloRequirement);
                                    }
                                }
                            }
                        }
                    }

                    componentCaml.put("requirements", requirements);
                    componentCaml.put("metrics", metrics);
                    componentsCaml.add(componentCaml);
                }
            }
        }

        List<Map<String, Object>> scopes = new ArrayList<>();
        Map<String, Object> scope = new LinkedHashMap<>();
        scope.put("name", "a_scope");
        scope.put("components", componentNames);
        scopes.add(scope);

        camlYaml.put("spec", Map.of("components", componentsCaml, "scopes", scopes));
        return camlYaml;
    }

    // Extracted legacy parser (original behavior)
    @SuppressWarnings("unchecked")
    protected Map<String, Object> translateToscaToCamlLegacy(Map<String, Object> toscaYaml, File inputFile) {
        Map<String, Object> camlYaml = new LinkedHashMap<>();
        camlYaml.put("apiVersion", "nebulous/v1");
        camlYaml.put("kind", "MetricModel");
        log.info("Extracting components (legacy)");
        List<Map<String, Object>> componentsCaml = new ArrayList<>();

        Map<String, Object> metadata = (Map<String, Object>) toscaYaml.get("metadata");
        if (metadata == null) {
            metadata = new LinkedHashMap<>();
        }
        metadata.put("fileName", inputFile.getName());
        camlYaml.put("metadata", metadata);

        Map<String, Object> componentsData = (Map<String, Object>) toscaYaml.get("node_types");
        List<String> componentNames = new ArrayList<>();

        if (componentsData != null) {
            for (Map.Entry<String, Object> entry : componentsData.entrySet()) {
                Map<String, Object> componentCaml = new LinkedHashMap<>();
                String componentName = entry.getKey();
                componentCaml.put("name", componentName);
                componentNames.add(componentName);

                Map<String, Object> componentNodeTypes = (Map<String, Object>) entry.getValue();
                List<Map<String, Object>> metrics = new ArrayList<>();
                List<Map<String, Object>> requirements = new ArrayList<>();

                // Loop through the node types of the component
                for (Map.Entry<String, Object> nodeType : componentNodeTypes.entrySet()) {
                    Map<String, Object> nodeData = (Map<String,
                            Object>) nodeType.getValue();
                    log.debug("Node data for {}: {}", entry.getKey(), nodeData);
                    List<Map<String, Object>> capabilities = (List<Map<String, Object>>) nodeData.get("capabilities");
                    log.debug("Capabilities for {}: {}", entry.getKey(), capabilities);

                    if (capabilities != null) {
                        log.info("Processing capabilities for {}", entry.getKey());
                        for (Map<String, Object> capability : capabilities) {
                            capability = (Map<String, Object>) capability.entrySet().iterator().next().getValue();

                            log.debug("{} Capability: {}", capability.get("type"), capability);
                            // search for a capability which has "type" of capabilities.MetricMonitoringCapability
                            if ("capabilities.MetricMonitoringCapability".equals(capability.get("type"))) {
                                Map<String, Object> monitoringProperties = (Map<String, Object>) capability.get("properties");
                                if (monitoringProperties != null) {
                                    // Process raw metrics
                                    List<Map<String, Object>> raw = (List<Map<String, Object>>) monitoringProperties.get("raw");
                                    log.debug("Raw metrics for {}: {}", entry.getKey(), raw);

                                    if (raw != null) {
                                        for (Map<String, Object> rawMetric : raw) {
                                            Map.Entry<String, Object> rawMetricEntry = (Map.Entry<String, Object>) rawMetric.entrySet().iterator().next();
                                            log.debug("Raw metric: {}", rawMetric);
                                            Map<String, Object> metric = new LinkedHashMap<>();
                                            metric.put("name", rawMetricEntry.getKey());

                                            Map<String, Object> rawMetricData = (Map<String, Object>) rawMetricEntry.getValue();
                                            if (rawMetricData != null) {
                                                // Sensor configuration (legacy uses collector/collector_inst)
                                                Map<String, Object> sensor = new LinkedHashMap<>();
                                                sensor.put("type", rawMetricData.get("collector"));
                                                Map<String, Object> sensorConfig = (Map<String, Object>) rawMetricData.get("config");
                                                log.debug("Sensor config: {}", sensorConfig);

                                                String collectorInstance = (String) rawMetricData.get("collector_inst");
                                                if (sensorConfig == null) {
                                                    sensorConfig = new LinkedHashMap<>();
                                                }
                                                sensorConfig.put("metric", collectorInstance);
                                                sensor.put("config", sensorConfig);
                                                metric.put("sensor", sensor);

                                                // Output and frequency
                                                String collectionOutput = (String) rawMetricData.get("collection_output");
                                                String collectionFrequency = (String) rawMetricData.get("collection_frequency");
                                                metric.put("output", collectionOutput + " " + collectionFrequency);
                                            }

                                            // Add to metrics
                                            metrics.add(metric);
                                        }
                                    }

                                    // Process composite metrics
                                    List<Map<String, Object>> composite = (List<Map<String, Object>>) monitoringProperties.get("composite");
                                    log.debug("Composite metrics for {}: {}", entry.getKey(), composite);

                                    if (composite != null) {
                                        for (Map<String, Object> compositeMetric : composite) {
                                            Map.Entry<String, Object> compositeMetricEntry = (Map.Entry<String, Object>) compositeMetric.entrySet().iterator().next();
                                            Map<String, Object> metric = new LinkedHashMap<>();
                                            metric.put("name", compositeMetricEntry.getKey());
                                            Map<String, Object> compositeData = (Map<String, Object>) compositeMetricEntry.getValue();

                                            if (compositeData != null) {
                                                // Handle the formula
                                                Map<String, Object> formula = (Map<String, Object>) compositeData.get("formula");
                                                if (formula != null) {
                                                    String collectionOutput = (String) formula.get("collection_output");
                                                    String collectionFrequency = (String) formula.get("collection_frequency");
                                                    metric.put("formula", formula.get("type") + "(" + formula.get("argument") + ")");
                                                    metric.put("output", collectionOutput + " " + collectionFrequency);
                                                }

                                                // Handle window
                                                Map<String, Object> window = (Map<String, Object>) compositeData.get("window");
                                                if (window != null) {
                                                    String windowType = (String) window.get("type");
                                                    String windowSize = (String) window.get("size");
                                                    metric.put("window", windowType + " " + windowSize);
                                                }

                                                // Add output and frequency
                                                Map<String, Object> processing = (Map<String, Object>) compositeData.get("processing");
                                                if (processing != null) {
                                                    String processingType = (String) processing.get("type");
                                                    String processingCriteria = (String) processing.get("criteria");
                                                    metric.put(processingType, processingCriteria);
                                                }

                                                // Add to metrics
                                                metrics.add(metric);

                                            }
                                        }
                                    }
                                }
                            }

                            // Requirements
                            if ("capabilities.SloMonitoringCapability".equals(capability.get("type"))) {
                                List<Map<String, Object>> sloProperties = (List<Map<String, Object>>) capability.get("properties");
                                if (sloProperties != null) {
                                    log.debug("SLO properties for {}: {}", entry.getKey(), sloProperties);
                                    for (Map<String, Object> slo : sloProperties) {
                                        Map.Entry<String, Object> sloEntry = (Map.Entry<String, Object>) slo.entrySet().iterator().next();
                                        Map<String, Object> sloRequirement = new LinkedHashMap<>();
                                        sloRequirement.put("name", sloEntry.getKey());
                                        sloRequirement.put("type", "slo");
                                        Map<String, Object> sloData = (Map<String, Object>) sloEntry.getValue();
                                        sloRequirement.put("constraint", sloData.get("constraint"));
                                        requirements.add(sloRequirement);
                                    }
                                }
                            }
                        }
                    }

                    componentCaml.put("requirements", requirements);
                    componentCaml.put("metrics", metrics);
                    componentsCaml.add(componentCaml);
                }
            }
        }

        List<Map<String, Object>> scopes = new ArrayList<>();
        Map<String, Object> scope = new LinkedHashMap<>();
        scope.put("name", "a_scope");
        scope.put("components", componentNames);
        scopes.add(scope);
        camlYaml.put("spec", Map.of("components", componentsCaml, "scopes", scopes));
        return camlYaml;
    }

    // TOSCA2 parser for Swarmchestrate TOSCA 2.0 format
    @SuppressWarnings("unchecked")
    protected Map<String, Object> translateToscaToCamlTOSCA2(Map<String, Object> toscaYaml, File inputFile) {
        Map<String, Object> camlYaml = new LinkedHashMap<>();
        camlYaml.put("apiVersion", "nebulous/v1");
        camlYaml.put("kind", "MetricModel");

        log.info("Creating metadata section");
        // Extract metadata from TOSCA file if present
        Map<String, Object> metadata = (Map<String, Object>) toscaYaml.get("metadata");
        if (metadata == null) {
            metadata = new LinkedHashMap<>();
        } else {
            // Create a copy to avoid modifying the original
            metadata = new LinkedHashMap<>(metadata);
        }
        String fileName = inputFile.getName();
        metadata.put("fileName", fileName);
        camlYaml.put("metadata", metadata);

        log.info("Extracting components from node_templates");
        List<Map<String, Object>> componentsCaml = new ArrayList<>();

        // Get service_template
        Map<String, Object> serviceTemplate = (Map<String, Object>) toscaYaml.get("service_template");
        if (serviceTemplate == null) {
            log.warn("No service_template found in TOSCA YAML");
            return null;
        }

        // Get node_templates
        Map<String, Object> nodeTemplates = (Map<String, Object>) serviceTemplate.get("node_templates");
        if (nodeTemplates == null) {
            log.warn("No node_templates found in service_template");
            return null;
        }

        log.info("Processing node_templates");
        for (Map.Entry<String, Object> entry : nodeTemplates.entrySet()) {
            String nodeName = entry.getKey();
            Map<String, Object> nodeData = (Map<String, Object>) entry.getValue();

            log.info("Processing node: {}", nodeName);

            // Process capabilities
            Map<String, Object> capabilities = (Map<String, Object>) nodeData.get("capabilities");
            if (capabilities == null) {
                log.info("Skipping node {}: no capabilities found", nodeName);
                continue; // Skip this node
            }

            Map<String, Object> componentCaml = new LinkedHashMap<>();
            componentCaml.put("name", nodeName);

            List<Map<String, Object>> metrics = new ArrayList<>();
            List<Map<String, Object>> requirements = new ArrayList<>();

            { // Start capabilities processing block

            // Process metrics capability
            Map<String, Object> metricsCapability = (Map<String, Object>) capabilities.get("metrics");
                if (metricsCapability != null) {
                    log.debug("Found metrics capability");
                    Map<String, Object> metricsProperties = (Map<String, Object>) metricsCapability.get("properties");
                    if (metricsProperties != null) {
                        // Process raw metrics
                        List<Map<String, Object>> rawMetrics = (List<Map<String, Object>>) metricsProperties.get("raw");
                        if (rawMetrics != null) {
                            log.debug("Processing raw metrics");
                            for (Map<String, Object> rawMetric : rawMetrics) {
                                Map<String, Object> metric = processRawMetricTOSCA2(rawMetric);
                                if (metric != null) {
                                    metrics.add(metric);
                                }
                            }
                        }

                        // Process composite metrics
                        List<Map<String, Object>> compositeMetrics = (List<Map<String, Object>>) metricsProperties.get("composite");
                        if (compositeMetrics != null) {
                            log.debug("Processing composite metrics");
                            for (Map<String, Object> compositeMetric : compositeMetrics) {
                                Map<String, Object> metric = processCompositeMetricTOSCA2(compositeMetric);
                                if (metric != null) {
                                    metrics.add(metric);
                                }
                            }
                        }
                    }
                }

                // Process slo-constraints capability
                Map<String, Object> sloConstraints = (Map<String, Object>) capabilities.get("slo-constraints");
                if (sloConstraints != null) {
                    log.debug("Found slo-constraints capability");
                    Map<String, Object> sloProperties = (Map<String, Object>) sloConstraints.get("properties");
                    if (sloProperties != null) {
                        log.debug("Processing SLO constraints");
                        // Handle or_list for multiple constraints
                        List<Map<String, Object>> orList = (List<Map<String, Object>>) sloProperties.get("or_list");
                        if (orList != null) {
                            for (Map<String, Object> sloItem : orList) {
                                Map<String, Object> requirement = processSLOConstraintTOSCA2(sloItem);
                                if (requirement != null) {
                                    requirements.add(requirement);
                                }
                            }
                        } else {
                            // Fallback for single constraint
                            Map<String, Object> requirement = processSLOConstraintTOSCA2(sloProperties);
                            if (requirement != null) {
                                requirements.add(requirement);
                            }
                        }
                    }
                }
            }

            // Add requirements before metrics (matching EMS format)
            if (!requirements.isEmpty()) {
                componentCaml.put("requirements", requirements);
            }
            if (!metrics.isEmpty()) {
                componentCaml.put("metrics", metrics);
            }

            componentsCaml.add(componentCaml);
        }

        // Create spec with components (matching EMS format)
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("components", componentsCaml);
        camlYaml.put("spec", spec);
        log.debug("Components: {}", componentsCaml);

        return camlYaml;
    }

    /**
     * Process a raw metric from TOSCA2 format
     * Handles two formats:
     * 1. Metric as direct object with "name" field: {name: "metric1", sensor: "...", ...}
     * 2. Metric keyed by name: {metric_name: {collector: "...", config: {...}}}
     */
    private Map<String, Object> processRawMetricTOSCA2(Map<String, Object> rawMetric) {
        log.debug("Processing raw metric: {}", rawMetric);
        Map<String, Object> metric = new LinkedHashMap<>();

        String name = (String) rawMetric.get("name");
        Map<String, Object> metricData = rawMetric;

        // Handle format where metric name is the key (legacy format)
        if (name == null && rawMetric.size() == 1) {
            String keyName = rawMetric.keySet().iterator().next();
            Object value = rawMetric.get(keyName);
            if (value instanceof Map) {
                name = keyName;
                metricData = (Map<String, Object>) value;
                log.debug("Detected legacy format: metric name '{}' is the key", name);
            }
        }

        if (name == null) {
            log.warn("Raw metric missing name field");
            return null;
        }

        metric.put("name", name);

        // Sensor configuration
        Map<String, Object> sensor = new LinkedHashMap<>();
        String sensorType = (String) metricData.get("sensor");
        
        // Handle legacy format where "collector" is used instead of "sensor"
        if (sensorType == null) {
            sensorType = (String) metricData.get("collector");
        }
        
        if (sensorType != null) {
            sensor.put("type", sensorType);
        }

        Map<String, Object> sensorConfig = (Map<String, Object>) metricData.get("config");
        if (sensorConfig != null) {
            // Create a copy to avoid modifying the original
            Map<String, Object> configCopy = new LinkedHashMap<>(sensorConfig);
            sensor.put("config", configCopy);
        }

        metric.put("sensor", sensor);

        // Output and frequency
        String collectionOutput = (String) metricData.get("collection_output");
        String collectionFrequency = (String) metricData.get("collection_frequency");
        if (collectionOutput != null && collectionFrequency != null) {
            metric.put("output", collectionOutput + " " + collectionFrequency);
        }

        return metric;
    }

    /**
     * Process a composite metric from TOSCA2 format
     * Handles two formats:
     * 1. Metric as direct object with "name" field: {name: "metric1", formula: "...", ...}
     * 2. Metric keyed by name: {metric_name: {formula: {...}, window: {...}}}
     */
    private Map<String, Object> processCompositeMetricTOSCA2(Map<String, Object> compositeMetric) {
        log.debug("Processing composite metric: {}", compositeMetric);
        Map<String, Object> metric = new LinkedHashMap<>();

        String name = (String) compositeMetric.get("name");
        Map<String, Object> metricData = compositeMetric;

        // Handle format where metric name is the key (legacy format)
        if (name == null && compositeMetric.size() == 1) {
            String keyName = compositeMetric.keySet().iterator().next();
            Object value = compositeMetric.get(keyName);
            if (value instanceof Map) {
                name = keyName;
                metricData = (Map<String, Object>) value;
                log.debug("Detected legacy format: composite metric name '{}' is the key", name);
            }
        }

        if (name == null) {
            log.warn("Composite metric missing name field");
            return null;
        }

        metric.put("name", name);

        // Formula - handle both direct formula string and nested formula object
        Object formulaObj = metricData.get("formula");
        if (formulaObj != null) {
            if (formulaObj instanceof Map) {
                // Formula is an object with type and argument
                Map<String, Object> formulaMap = (Map<String, Object>) formulaObj;
                String formulaType = (String) formulaMap.get("type");
                String formulaArg = (String) formulaMap.get("argument");
                if (formulaType != null && formulaArg != null) {
                    metric.put("formula", formulaType + "( " + formulaArg + " )");
                }
            } else {
                // Formula is a direct string
                metric.put("formula", formulaObj.toString());
            }
        }

        // Output and frequency
        String collectionOutput = (String) metricData.get("collection_output");
        String collectionFrequency = (String) metricData.get("collection_frequency");
        if (collectionOutput != null && collectionFrequency != null) {
            metric.put("output", collectionOutput + " " + collectionFrequency);
        }

        // Window - handle both direct fields and nested window object
        String windowType = (String) metricData.get("window_type");
        String windowSize = (String) metricData.get("window_size");
        
        if ((windowType == null || windowSize == null)) {
            Object windowObj = metricData.get("window");
            if (windowObj instanceof Map) {
                Map<String, Object> windowMap = (Map<String, Object>) windowObj;
                if (windowType == null) {
                    windowType = (String) windowMap.get("type");
                }
                if (windowSize == null) {
                    windowSize = (String) windowMap.get("size");
                }
            }
        }
        
        if (windowType != null && windowSize != null) {
            metric.put("window", windowType + " " + windowSize);
        }

        // Grouping
        Object grouping = metricData.get("grouping");
        if (grouping != null) {
            metric.put("grouping", grouping.toString());
        }

        return metric;
    }

    /**
     * Process SLO constraint from TOSCA2 format
     * Note: In TOSCA2 format, slo-constraints is a single object, not a list
     */
    private Map<String, Object> processSLOConstraintTOSCA2(Map<String, Object> sloProperties) {
        log.debug("Processing SLO constraint: {}", sloProperties);
        Map<String, Object> requirement = new LinkedHashMap<>();

        String name = (String) sloProperties.get("name");
        if (name == null) {
            log.warn("SLO constraint missing name field");
            return null;
        }

        requirement.put("name", name);
        requirement.put("type", "slo");

        // Build constraint from metric, operator, and threshold
        String metric = (String) sloProperties.get("metric");
        String operator = (String) sloProperties.get("operator");
        Object threshold = sloProperties.get("threshold");

        if (metric != null && operator != null && threshold != null) {
            String constraint = metric + " " + operator + " " + threshold;
            requirement.put("constraint", constraint);
        } else {
            log.warn("SLO constraint missing required fields (metric, operator, threshold)");
            // Try to use a direct constraint field if it exists
            Object directConstraint = sloProperties.get("constraint");
            if (directConstraint != null) {
                requirement.put("constraint", directConstraint.toString());
            }
        }

        return requirement;
    }
}