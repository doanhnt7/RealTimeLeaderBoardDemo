package jobs.deserializer;

import jobs.models.User;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.time.OffsetDateTime;

public class UserDeser implements KafkaRecordDeserializationSchema<User> {
    private static final ObjectMapper M = new ObjectMapper();

	@Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<User> out) throws IOException {
        try {
            String value = new String(record.value(), StandardCharsets.UTF_8);
            JsonNode root = M.readTree(value);

            // Debezium MongoDB payload wrapper
            JsonNode payload = root.has("payload") ? root.get("payload") : root;

            String beforeStr = payload.has("before") && !payload.get("before").isNull() ? payload.get("before").asText() : null;
            String afterStr = payload.has("after") && !payload.get("after").isNull() ? payload.get("after").asText() : null;

            if (afterStr == null || afterStr.isEmpty()) {
                return; // nothing to emit if we don't have the current state
            }

            JsonNode after = M.readTree(afterStr);
            JsonNode before = (beforeStr != null && !beforeStr.isEmpty()) ? M.readTree(beforeStr) : null;

            User u = new User();

            // _id can be string or object {"$oid": "..."}
            if (after.has("_id")) {
                u.set_id(parseId(after.get("_id")));
            }
            if (after.has("uid")) u.setUid(after.get("uid").isNull() ? null : after.get("uid").asText());
            if (after.has("email")) u.setEmail(after.get("email").isNull() ? null : after.get("email").asText());
            if (after.has("authProvider")) u.setAuthProvider(after.get("authProvider").isNull() ? null : after.get("authProvider").asText());
            if (after.has("appId")) u.setAppId(after.get("appId").isNull() ? null : after.get("appId").asText());
            if (after.has("avatar")) u.setAvatar(after.get("avatar").isNull() ? null : after.get("avatar").asText());
            if (after.has("geo")) u.setGeo(after.get("geo").isNull() ? null : after.get("geo").asText());
            if (after.has("role")) u.setRole(after.get("role").isNull() ? null : after.get("role").asText());
            if (after.has("name")) u.setName(after.get("name").isNull() ? null : after.get("name").asText());

            if (after.has("devices") && after.get("devices").isArray()) {
                u.setDevices(M.readerForListOf(User.Device.class).readValue(after.get("devices")));
            }
            if (after.has("resources") && after.get("resources").isArray()) {
                u.setResources(M.readerForListOf(Object.class).readValue(after.get("resources")));
            }

            if (after.has("created_at")) u.setCreated_at(parseDate(after.get("created_at")));
            if (after.has("updated_at")) u.setUpdated_at(parseDate(after.get("updated_at")));
            if (after.has("lastLoginAt")) u.setLastLoginAt(parseDate(after.get("lastLoginAt")));
            if (after.has("updatedAt")) u.setUpdatedAt(parseDate(after.get("updatedAt")));

            if (after.has("level")) u.setLevel(after.get("level").isNull() ? 0 : after.get("level").asInt());
            if (after.has("team")) u.setTeam(after.get("team").isNull() ? 0 : after.get("team").asInt());

            // previousLevel from BEFORE.level if available
            if (before != null && before.has("level") && !before.get("level").isNull()) {
                u.setPreviousLevel(before.get("level").asInt());
            }

            if (u.getUid() != null) out.collect(u);
        } catch (Exception ignored) {
            // swallow bad records
        }
    }

	@Override
	public TypeInformation<User> getProducedType() {
		return TypeInformation.of(User.class);
	}

    private OffsetDateTime parseDate(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            // Mongo extended JSON: { "$date": NUMBER | STRING }
            if (node.isObject() && node.has("$date")) {
                JsonNode d = node.get("$date");
                if (d.isNumber()) {
                    long ms = d.asLong();
                    return OffsetDateTime.ofInstant(java.time.Instant.ofEpochMilli(ms), java.time.ZoneOffset.UTC);
                }
                if (d.isTextual()) {
                    String s = d.asText();
                    // Try instant first, then OffsetDateTime
                    try {
                        return OffsetDateTime.ofInstant(java.time.Instant.parse(s), java.time.ZoneOffset.UTC);
                    } catch (Exception ignored) {
                        return OffsetDateTime.parse(s);
                    }
                }
            }
            // Plain number millis
            if (node.isNumber()) {
                long ms = node.asLong();
                return OffsetDateTime.ofInstant(java.time.Instant.ofEpochMilli(ms), java.time.ZoneOffset.UTC);
            }
            // Plain ISO text
            if (node.isTextual()) {
                String s = node.asText();
                try {
                    return OffsetDateTime.parse(s);
                } catch (Exception ignored) {
                    return OffsetDateTime.ofInstant(java.time.Instant.parse(s), java.time.ZoneOffset.UTC);
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private String parseId(JsonNode idNode) {
        if (idNode == null || idNode.isNull()) return null;
        if (idNode.isObject() && idNode.has("$oid") && idNode.get("$oid").isTextual()) {
            return idNode.get("$oid").asText();
        }
        return idNode.asText();
    }
}
