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
//@Service
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
        Map<String, Object> camlYaml = new LinkedHashMap<>();
        camlYaml.put("apiVersion", "nebulous/v1");
        camlYaml.put("kind", "MetricModel");
        log.info("Extracting components");
        // Extract components
        List<Map<String, Object>> componentsCaml = new ArrayList<>();

        log.info("Creating metadata section");
        // Metadata section
        Map<String, Object> metadata = (Map<String, Object>) toscaYaml.get("metadata");
        if (metadata == null) {
            metadata = new LinkedHashMap<>();
        }
        String fileName = inputFile.getName();
        metadata.put("fileName", fileName);
        log.debug("Metadata: {}", metadata);
        camlYaml.put("metadata", metadata);

        Map<String, Object> componentsData = (Map<String, Object>) toscaYaml.get("node_types");
        log.debug("Component types: {}", componentsData);

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
                                                // Sensor configuration
                                                Map<String, Object> sensor = new LinkedHashMap<>();
                                                sensor.put("type", rawMetricData.get("collector"));
                                                Map<String, Object> sensorConfig = (Map<String, Object>) rawMetricData.get("config");
                                                log.debug("Sensor config: {}", sensorConfig);

                                                String collectorInstance = (String) rawMetricData.get("collector_inst");
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

                    // Add metrics to componentCaml
                    componentCaml.put("metrics", metrics);
                    componentsCaml.add(componentCaml);
                }
            }
        }

        log.info("Creating scopes section");
        // Scopes section
        List<Map<String, Object>> scopes = new ArrayList<>();
        Map<String, Object> scope = new LinkedHashMap<>();
        scope.put("name", "a_scope");
        scope.put("components", componentNames);
        scopes.add(scope);
        log.debug("Scopes: {}", scopes);

        //spec contains components and scopes
        camlYaml.put("spec", Map.of("components", componentsCaml, "scopes", scopes));
        log.debug("Components: {}", componentsCaml);

        return camlYaml;
    }
}