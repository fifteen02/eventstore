package com.fifteen02.eventstore.config;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LogbackProfileConfigTest {

    @Test
    void devProfileDoesNotConfigureGoogleCloudLoggingAppender() throws Exception {
        var profile = springProfile("dev");

        assertThat(hasCloudLoggingAppender(profile)).isFalse();
        assertThat(hasAppenderRef(profile, "CLOUD")).isFalse();
    }

    @Test
    void testProfileDoesNotConfigureGoogleCloudLoggingAppender() throws Exception {
        var profile = springProfile("test");

        assertThat(hasCloudLoggingAppender(profile)).isFalse();
        assertThat(hasAppenderRef(profile, "CLOUD")).isFalse();
    }

    @Test
    void stagingAndLiveProfilesConfigureGoogleCloudLoggingAppender() throws Exception {
        assertThat(hasCloudLoggingAppender(springProfile("staging"))).isTrue();
        assertThat(hasAppenderRef(springProfile("staging"), "CLOUD")).isTrue();
        assertThat(hasCloudLoggingAppender(springProfile("live"))).isTrue();
        assertThat(hasAppenderRef(springProfile("live"), "CLOUD")).isTrue();
    }

    private Element springProfile(String profileName) throws Exception {
        var document = logbackConfig();
        var profiles = document.getElementsByTagName("springProfile");

        return IntStream.range(0, profiles.getLength())
                .mapToObj(profiles::item)
                .map(Element.class::cast)
                .filter(profile -> includesProfile(profile, profileName))
                .findFirst()
                .orElseThrow();
    }

    private Document logbackConfig() throws Exception {
        var factory = DocumentBuilderFactory.newInstance();
        var builder = factory.newDocumentBuilder();
        try (var input = getClass().getResourceAsStream("/logback-spring.xml")) {
            return builder.parse(input);
        }
    }

    private boolean includesProfile(Element profile, String profileName) {
        return names(profile).anyMatch(profileName::equals);
    }

    private boolean hasCloudLoggingAppender(Element profile) {
        return elements(profile, "appender")
                .anyMatch(appender -> "com.google.cloud.logging.logback.LoggingAppender"
                        .equals(appender.getAttribute("class")));
    }

    private boolean hasAppenderRef(Element profile, String appenderName) {
        return elements(profile, "appender-ref")
                .anyMatch(appenderRef -> appenderName.equals(appenderRef.getAttribute("ref")));
    }

    private Stream<String> names(Element profile) {
        return Arrays.stream(profile.getAttribute("name").split(","))
                .map(String::trim);
    }

    private Stream<Element> elements(Element profile, String tagName) {
        var elements = profile.getElementsByTagName(tagName);
        return IntStream.range(0, elements.getLength())
                .mapToObj(elements::item)
                .map(Element.class::cast);
    }
}
