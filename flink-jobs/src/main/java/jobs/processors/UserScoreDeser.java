package jobs.processors;

import jobs.models.UserScore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.time.OffsetDateTime;

public class UserScoreDeser implements KafkaRecordDeserializationSchema<UserScore> {
    private static final ObjectMapper M = new ObjectMapper();

	@Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UserScore> out) throws IOException {
        try {
            String value = new String(record.value(), StandardCharsets.UTF_8);
            JsonNode n = M.readTree(value);

            UserScore u = new UserScore();
            if (n.has("_id")) u.set_id(n.get("_id").isNull() ? null : n.get("_id").asText());
            if (n.has("uid")) u.setUid(n.get("uid").isNull() ? null : n.get("uid").asText());
            if (n.has("email")) u.setEmail(n.get("email").isNull() ? null : n.get("email").asText());
            if (n.has("authProvider")) u.setAuthProvider(n.get("authProvider").isNull() ? null : n.get("authProvider").asText());
            if (n.has("appId")) u.setAppId(n.get("appId").isNull() ? null : n.get("appId").asText());
            if (n.has("avatar")) u.setAvatar(n.get("avatar").isNull() ? null : n.get("avatar").asText());
            if (n.has("geo")) u.setGeo(n.get("geo").isNull() ? null : n.get("geo").asText());
            if (n.has("role")) u.setRole(n.get("role").isNull() ? null : n.get("role").asText());
            if (n.has("lastLoginAt")) u.setLastLoginAt(n.get("lastLoginAt").isNull() ? null : OffsetDateTime.parse(n.get("lastLoginAt").asText()));
            if (n.has("name")) u.setName(n.get("name").isNull() ? null : n.get("name").asText());
            if (n.has("devices") && n.get("devices").isArray()) {
                // Let jackson bind this array via POJO mapping for simplicity
                u.setDevices(M.readerForListOf(UserScore.Device.class).readValue(n.get("devices")));
            }
            if (n.has("resources") && n.get("resources").isArray()) {
                u.setResources(M.readerForListOf(Object.class).readValue(n.get("resources")));
            }
            if (n.has("created_at")) u.setCreated_at(n.get("created_at").isNull() ? null : OffsetDateTime.parse(n.get("created_at").asText()));
            if (n.has("updated_at")) u.setUpdated_at(n.get("updated_at").isNull() ? null : OffsetDateTime.parse(n.get("updated_at").asText()));
            if (n.has("level")) u.setLevel(n.get("level").isNull() ? 0 : n.get("level").asInt());
            if (n.has("updatedAt")) u.setUpdatedAt(n.get("updatedAt").isNull() ? null : OffsetDateTime.parse(n.get("updatedAt").asText()));
            if (n.has("team")) u.setTeam(n.get("team").isNull() ? 0 : n.get("team").asInt());

            if (u.getUid() != null) out.collect(u);
        } catch (Exception ignored) {
            // swallow bad records
        }
    }

	@Override
	public TypeInformation<UserScore> getProducedType() {
		return TypeInformation.of(UserScore.class);
	}
}
