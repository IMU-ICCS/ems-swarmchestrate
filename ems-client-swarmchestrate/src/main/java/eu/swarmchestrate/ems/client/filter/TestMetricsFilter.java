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

import java.time.LocalTime;

@Slf4j
@Service
public class TestMetricsFilter implements IMetricsFilter, InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info(">>>>>>>>>>>>>>>>> [SWC] TestMetricFilter initialized");
    }

    @Override
    public Context prePublishMetric(String metricName, double metricValue, long timestamp, String nodeAddress) {
        log.info(">>>>>>>>>>>>>>>>> [SWC] prePublishMetric: name={}, value={}, ts={}, nodeAddress={}", metricName, metricValue, timestamp, nodeAddress);
        return new IMetricsFilter.Context(LocalTime.now().getSecond()<30, this);
    }

    @Override
    public void postPublishMetric(String metricName, double metricValue, long timestamp, String nodeAddress, Context context) {
        log.info(">>>>>>>>>>>>>>>>> [SWC] postPublishMetric: name={}, value={}, ts={}, nodeAddress={}, context={}", metricName, metricValue, timestamp, nodeAddress, context);
    }

    @Override
    public void notPublishedMetric(String metricName, double metricValue, long timestamp, String nodeAddress, Context context) {
        log.info(">>>>>>>>>>>>>>>>> [SWC] notPublishedMetric: name={}, value={}, ts={}, nodeAddress={}, context={}", metricName, metricValue, timestamp, nodeAddress, context);
    }
}
