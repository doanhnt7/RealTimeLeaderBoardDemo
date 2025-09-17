package jobs;

import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.DataTypes;

public class TableDescriptors {
    
    public static TableDescriptor createUsersSourceDescriptor() {
        return TableDescriptor
            .forConnector("kafka")
            .schema(Schema.newBuilder()
                .column("_id", DataTypes.STRING())
                .column("uid", DataTypes.STRING())
                .column("email", DataTypes.STRING())
                .column("authProvider", DataTypes.STRING())
                .column("appId", DataTypes.STRING())
                .column("avatar", DataTypes.STRING())
                .column("geo", DataTypes.STRING())
                .column("role", DataTypes.STRING())
                .column("lastLoginAt", DataTypes.STRING())
                .column("name", DataTypes.STRING())
                .column("devices", DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))
                .column("resources", DataTypes.ARRAY(DataTypes.STRING()))
                .column("created_at", DataTypes.STRING())
                .column("updated_at", DataTypes.STRING())
                .column("level", DataTypes.INT())
                .column("updatedAt", DataTypes.STRING())
                .column("teamId", DataTypes.INT())
                .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                .watermark("ts", "ts - INTERVAL '5' SECOND")
                .build())
            .option("topic", "users")
            .option("properties.bootstrap.servers", "kafka:29092")
            .option("properties.group.id", "leaderboard-consumer")
            .option("scan.startup.mode", "earliest-offset")
            .option("format", "json")
            .option("json.ignore-parse-errors", "true")
            .build();
    }
    
    public static TableDescriptor createTopTeamsSinkDescriptor() {
        return TableDescriptor
            .forConnector("upsert-kafka")
            .schema(Schema.newBuilder()
                .column("rnk", DataTypes.BIGINT())
                .column("team_id", DataTypes.STRING())
                .column("team_name", DataTypes.STRING())
                .column("total_score", DataTypes.BIGINT())
                .primaryKey("rnk")
                .build())
            .option("topic", "top-teams")
            .option("properties.bootstrap.servers", "kafka:29092")
            .option("key.format", "json")
            .option("value.format", "json")
            .build();
    }
    
    public static TableDescriptor createTopPlayersSinkDescriptor() {
        return TableDescriptor
            .forConnector("upsert-kafka")
            .schema(Schema.newBuilder()
                .column("rnk", DataTypes.BIGINT())
                .column("user_id", DataTypes.STRING())
                .column("team_name", DataTypes.STRING())
                .column("total_score", DataTypes.BIGINT())
                .primaryKey("rnk")
                .build())
            .option("topic", "top-players")
            .option("properties.bootstrap.servers", "kafka:29092")
            .option("key.format", "json")
            .option("value.format", "json")
            .build();
    }
    
    public static TableDescriptor createHotStreakersSinkDescriptor() {
        return TableDescriptor
            .forConnector("upsert-kafka")
            .schema(Schema.newBuilder()
                .column("rnk", DataTypes.BIGINT())
                .column("user_id", DataTypes.STRING())
                .column("short_term_avg", DataTypes.DOUBLE())
                .column("long_term_avg", DataTypes.DOUBLE())
                .column("peak_hotness", DataTypes.DOUBLE())
                .primaryKey("rnk")
                .build())
            .option("topic", "hot-streakers")
            .option("properties.bootstrap.servers", "kafka:29092")
            .option("key.format", "json")
            .option("value.format", "json")
            .build();
    }
    
    public static TableDescriptor createTeamMvpsSinkDescriptor() {
        return TableDescriptor
            .forConnector("upsert-kafka")
            .schema(Schema.newBuilder()
                .column("rnk", DataTypes.BIGINT())
                .column("user_id", DataTypes.STRING())
                .column("team_name", DataTypes.STRING())
                .column("player_total", DataTypes.BIGINT())
                .column("team_total", DataTypes.BIGINT())
                .column("contrib_ratio", DataTypes.DOUBLE())
                .primaryKey("rnk")
                .build())
            .option("topic", "team-mvps")
            .option("properties.bootstrap.servers", "kafka:29092")
            .option("key.format", "json")
            .option("value.format", "json")
            .build();
    }
}
