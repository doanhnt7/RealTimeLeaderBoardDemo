package jobs.queries;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Over;
import static org.apache.flink.table.api.Expressions.*;

public class HotStreakersQuery {
    
    public static Table createHotStreakersTable(TableEnvironment tableEnv, Table userScores) {
        // Hot Streakers Query using Table API for window functions
        Table shortTermAgg = userScores
            .window(Over.partitionBy($("user_id"))
                .orderBy($("event_time"))
                .preceding(lit(10).second())
                .as("w"))
            .select(
                $("user_id"),
                $("event_time"),
                $("score"),
                $("score").sum().over($("w")).as("short_sum"),
                $("score").count().over($("w")).as("short_cnt")
            );

        Table longTermAgg = shortTermAgg
            .window(Over.partitionBy($("user_id"))
                .orderBy($("event_time"))
                .preceding(lit(60).second())
                .as("w"))
            .select(
                $("user_id"),
                $("event_time"),
                $("short_sum"),
                $("short_cnt"),
                $("score").sum().over($("w")).as("long_sum"),
                $("score").count().over($("w")).as("long_cnt")
            );

        // Calculate peak hotness using SQL for complex division
        tableEnv.createTemporaryView("long_term_agg", longTermAgg);
        
        Table peakHotness = tableEnv.sqlQuery(
            "SELECT user_id, " +
            "MAX(short_sum * 1.0 / NULLIF(short_cnt, 0)) AS short_term_avg, " +
            "MAX(long_sum * 1.0 / NULLIF(long_cnt, 0)) AS long_term_avg, " +
            "MAX((short_sum * 1.0 / NULLIF(short_cnt, 0)) / NULLIF((long_sum * 1.0 / NULLIF(long_cnt, 0)), 0)) AS peak_hotness " +
            "FROM long_term_agg GROUP BY user_id"
        );

        tableEnv.createTemporaryView("peak_hotness", peakHotness);
        
        return tableEnv.sqlQuery(
            "SELECT ROW_NUMBER() OVER (ORDER BY peak_hotness DESC, user_id ASC) AS rnk, " +
            "user_id, short_term_avg, long_term_avg, peak_hotness FROM peak_hotness WHERE ROW_NUMBER() OVER (ORDER BY peak_hotness DESC, user_id ASC) <= 10"
        );
    }
}
