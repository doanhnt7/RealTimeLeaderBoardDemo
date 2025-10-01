package jobs.deserializer;

import jobs.models.User;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collections;

public class UserDeserV2 implements KafkaRecordDeserializationSchema<User> {
    private static final ObjectMapper M = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<User> out) {
        try {
            if (record.value() == null) return;
            String value = new String(record.value(), StandardCharsets.UTF_8);
            JsonNode root = M.readTree(value);

            User u = new User();

            // Flat fields
            if (root.hasNonNull("uid")) u.setUid(root.get("uid").asText());
            if (root.has("email")) u.setEmail(root.get("email").isNull() ? null : root.get("email").asText());
            if (root.has("authProvider")) u.setAuthProvider(root.get("authProvider").isNull() ? null : root.get("authProvider").asText());
            if (root.has("appId")) u.setAppId(root.get("appId").isNull() ? null : root.get("appId").asText());
            if (root.has("avatar")) u.setAvatar(root.get("avatar").isNull() ? null : root.get("avatar").asText());
            if (root.has("geo")) u.setGeo(root.get("geo").isNull() ? null : root.get("geo").asText());
            if (root.has("role")) u.setRole(root.get("role").isNull() ? null : root.get("role").asText());
            if (root.has("name")) u.setName(root.get("name").isNull() ? null : root.get("name").asText());

            // devices: object -> list with one element to fit model
            if (root.has("devices") && root.get("devices").isObject()) {
                JsonNode d = root.get("devices");
                User.Device dev = new User.Device();
                if (d.has("fb_analytics_instance_id") && !d.get("fb_analytics_instance_id").isNull()) {
                    dev.setFb_analytics_instance_id(d.get("fb_analytics_instance_id").asText());
                }
                if (d.has("fb_instance_id") && !d.get("fb_instance_id").isNull()) {
                    dev.setFb_instance_id(d.get("fb_instance_id").asText());
                }
                if (d.has("fcmToken") && !d.get("fcmToken").isNull()) {
                    dev.setFcmToken(d.get("fcmToken").asText());
                }
                u.setDevices(Collections.singletonList(dev));
            }

            // resources: array
            if (root.has("resources") && root.get("resources").isArray()) {
                u.setResources(M.readerForListOf(Object.class).readValue(root.get("resources")));
            }

            // dates: ISO strings -> epoch millis
            if (root.has("lastLoginAt")) u.setLastLoginAt(parseIsoToMillis(root.get("lastLoginAt")));
            if (root.has("created_at")) u.setCreated_at(parseIsoToMillis(root.get("created_at")));
            if (root.has("updated_at")) u.setUpdated_at(parseIsoToMillis(root.get("updated_at")));
            if (root.has("updatedAt")) u.setUpdatedAt(parseIsoToMillis(root.get("updatedAt")));

            // numeric fields
            if (root.has("level") && root.get("level").canConvertToInt()) {
                u.setLevel(root.get("level").asInt());
            }
            if (root.has("team") && root.get("team").canConvertToInt()) {
                u.setTeam(root.get("team").asInt());
            }
            if (root.has("previousLevel") && root.get("previousLevel").canConvertToInt()) {
                u.setPreviousLevel(root.get("previousLevel").asInt());
            }

            if (u.getUid() != null) {
                out.collect(u);
            }
        } catch (Exception ignored) {
            // swallow bad records
        }
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(User.class);
    }

    private long parseIsoToMillis(JsonNode node) {
        if (node == null || node.isNull()) return 0L;
        try {
            String s = node.asText();
            try {
                return OffsetDateTime.parse(s).toInstant().toEpochMilli();
            } catch (Exception e) {
                return Instant.parse(s).toEpochMilli();
            }
        } catch (Exception e) {
            return 0L;
        }
    }
}
