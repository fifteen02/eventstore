package com.fifteen02.eventstore.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import com.fifteen02.eventstore.entity.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.StreamSupport;

@Component
@ConditionalOnProperty(name = "eventstore.big-query.enabled", havingValue = "true")
public class GoogleBigQueryClientImpl implements GoogleBigQueryClient {

    private static final Logger LOG = LoggerFactory.getLogger(GoogleBigQueryClientImpl.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final BigQuery bigQuery;
    private final String dataset;
    private final String table;

    public GoogleBigQueryClientImpl(
            @Value("${eventstore.big-query.dataset}") String dataset,
            @Value("${eventstore.big-query.table}") String table
    ) {
        this.bigQuery = BigQueryOptions.getDefaultInstance().getService();
        this.dataset = dataset;
        this.table = table;
    }

    @Override
    public List<Message> load(long fromInclusive, long toInclusive) throws InterruptedException {
        String query = """
                SELECT eventstore_id, user_id, traceparent, tracestate, baggage, metadata,
                       message_type, correlation_id, timestamp, content, content_hash, insert_time
                FROM `%s.%s`
                WHERE eventstore_id BETWEEN @fromInclusive AND @toInclusive
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY eventstore_id
                    ORDER BY insert_time DESC
                ) = 1
                ORDER BY eventstore_id;
                """.formatted(dataset, table);

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(query)
                .setUseLegacySql(false)
                .addNamedParameter("fromInclusive", QueryParameterValue.int64(fromInclusive))
                .addNamedParameter("toInclusive", QueryParameterValue.int64(toInclusive))
                .build();

        TableResult result = bigQuery.query(config);

        return StreamSupport.stream(result.iterateAll().spliterator(), false)
                .map(GoogleBigQueryClientImpl::toMessage)
                .sorted(Comparator.comparing(Message::getId))
                .toList();
    }

    /**
     * Fails on a row whose content cannot be restored, naming the id. Dropping the row instead only moved
     * the failure to the fetcher's range check, with no hint of which row or why.
     */
    static Message toMessage(FieldValueList row) {
        long id = row.get("eventstore_id").getLongValue();
        Message message = new Message();
        message.setId(id);
        message.setType(row.get("message_type").getStringValue());
        message.setContent(contentFrom(id, getOptionalString(row.get("content")).orElse(null)));
        message.setTimestamp(row.get("timestamp").getTimestampInstant());
        message.setContentHash(getOptionalString(row.get("content_hash")).orElse(null));
        message.setUserId(getOptionalString(row.get("user_id")).orElse(null));
        message.setTraceparent(getOptionalString(row.get("traceparent")).orElse(null));
        message.setTracestate(getOptionalString(row.get("tracestate")).orElse(null));
        message.setBaggage(getOptionalString(row.get("baggage")).orElse(null));
        message.setMetadata(getOptionalString(row.get("metadata"))
                .flatMap(GoogleBigQueryClientImpl::metadataFrom)
                .orElse(null));
        message.setCorrelationId(getOptionalString(row.get("correlation_id")).orElse(null));
        return message;
    }

    private static JsonNode contentFrom(long id, String value) {
        if (value == null) {
            throw corruptRow(id, "content is null", null);
        }
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (Exception exception) {
            throw corruptRow(id, "content is not valid JSON: " + exception.getMessage(), exception);
        }
    }

    private static IllegalStateException corruptRow(long id, String reason, Exception cause) {
        String message = "BigQuery row eventstore_id=" + id + " cannot be replayed: " + reason;
        LOG.error(message);
        return new IllegalStateException(message, cause);
    }

    private static Optional<Map<String, Object>> metadataFrom(String value) {
        try {
            return Optional.of(OBJECT_MAPPER.readValue(value, new TypeReference<Map<String, Object>>() {
            }));
        } catch (Exception exception) {
            return Optional.empty();
        }
    }

    private static <T> Optional<T> getOptional(
            FieldValue value,
            Function<FieldValue, T> extractor
    ) {
        return Optional.ofNullable(value)
                .filter(v -> !v.isNull())
                .map(extractor);
    }

    private static Optional<String> getOptionalString(FieldValue value) {
        return getOptional(value, FieldValue::getStringValue);
    }

}
