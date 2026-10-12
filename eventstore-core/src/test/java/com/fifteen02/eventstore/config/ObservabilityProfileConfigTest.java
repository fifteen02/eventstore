package com.fifteen02.eventstore.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ObservabilityProfileConfigTest {

    @Test
    void devProfileDisablesCloudMetricsAndTraceExport() throws Exception {
        var properties = propertiesFrom("application-dev.yml");

        assertExportDisabled(properties);
    }

    @Test
    void testProfileDisablesCloudMetricsAndTraceExport() throws Exception {
        var properties = propertiesFrom("application-test.yml");

        assertExportDisabled(properties);
    }

    private void assertExportDisabled(List<PropertySource<?>> properties) {
        assertThat(property(properties, "management.tracing.enabled")).contains(false);
        assertThat(property(properties, "management.defaults.metrics.export.enabled")).contains(false);
        assertThat(property(properties, "management.otlp.logging.export.enabled")).contains(false);
        assertThat(property(properties, "management.otlp.metrics.export.enabled")).contains(false);
        assertThat(property(properties, "management.otlp.tracing.export.enabled")).contains(false);
        assertThat(property(properties, "management.stackdriver.metrics.export.enabled")).contains(false);
        assertThat(property(properties, "management.simple.metrics.export.enabled")).contains(true);
    }

    private List<PropertySource<?>> propertiesFrom(String resourceName) throws Exception {
        var resource = new ClassPathResource(resourceName);
        var loader = new YamlPropertySourceLoader();
        return loader.load(resourceName, resource);
    }

    private Optional<Object> property(List<PropertySource<?>> properties, String name) {
        return properties.stream()
                .map(propertySource -> propertySource.getProperty(name))
                .filter(value -> value != null)
                .findFirst();
    }
}
