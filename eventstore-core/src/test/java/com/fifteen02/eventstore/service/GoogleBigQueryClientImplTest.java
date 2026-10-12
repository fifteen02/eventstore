package com.fifteen02.eventstore.service;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GoogleBigQueryClientImplTest {

    private static final FieldList SCHEMA = FieldList.of(
            Field.of("eventstore_id", LegacySQLTypeName.INTEGER),
            Field.of("message_type", LegacySQLTypeName.STRING),
            Field.of("content", LegacySQLTypeName.STRING),
            Field.of("timestamp", LegacySQLTypeName.TIMESTAMP),
            Field.of("content_hash", LegacySQLTypeName.STRING),
            Field.of("user_id", LegacySQLTypeName.STRING),
            Field.of("traceparent", LegacySQLTypeName.STRING),
            Field.of("tracestate", LegacySQLTypeName.STRING),
            Field.of("baggage", LegacySQLTypeName.STRING),
            Field.of("metadata", LegacySQLTypeName.STRING),
            Field.of("correlation_id", LegacySQLTypeName.STRING));

    @Test
    void toMessage_restoresAValidRow() throws Exception {
        var message = GoogleBigQueryClientImpl.toMessage(row(42L, "{\"ok\":true}"));

        assertThat(message.getId()).isEqualTo(42L);
        assertThat(message.getType()).isEqualTo("OrderShipped");
        assertThat(message.getContent()).isEqualTo(new ObjectMapper().readTree("{\"ok\":true}"));
    }

    @Test
    void toMessage_failsNamingTheIdWhenContentIsNotJson() {
        assertThatThrownBy(() -> GoogleBigQueryClientImpl.toMessage(row(42L, "{not json")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("eventstore_id=42")
                .hasMessageContaining("content is not valid JSON")
                .hasCauseInstanceOf(Exception.class);
    }

    @Test
    void toMessage_failsNamingTheIdWhenContentIsNull() {
        assertThatThrownBy(() -> GoogleBigQueryClientImpl.toMessage(row(43L, null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("eventstore_id=43")
                .hasMessageContaining("content is null");
    }

    private static FieldValueList row(long id, String content) {
        List<FieldValue> values = new ArrayList<>(List.of(
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, Long.toString(id)),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "OrderShipped"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, content),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1780484100.0")));
        for (int i = 0; i < 7; i++) {
            values.add(FieldValue.of(FieldValue.Attribute.PRIMITIVE, null));
        }
        return FieldValueList.of(values, SCHEMA);
    }
}
