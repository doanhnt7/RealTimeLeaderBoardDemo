package jobs.operators;

import jobs.models.Score;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink for Top-N snapshot side output. Each element is Tuple2<timestamp, List<Score>>.
 * Writes to MongoDB with _id = timestamp and an array of score documents.
 */
public class SnapshotTopNSink implements Sink<Tuple2<Long, List<Score>>> {

    private final String mongoUri;
    private final String database;
    private final String collection;

    public SnapshotTopNSink(String mongoUri, String database, String collection) {
        this.mongoUri = mongoUri;
        this.database = database;
        this.collection = collection;
    }

    @Override
    public SinkWriter<Tuple2<Long, List<Score>>> createWriter(WriterInitContext context) {
        return new SnapshotWriter(mongoUri, database, collection);
    }

    static class SnapshotWriter implements SinkWriter<Tuple2<Long, List<Score>>> {
        private final String mongoUri;
        private final String database;
        private final String collection;
        private transient MongoClient client;
        private transient MongoCollection<Document> col;

        SnapshotWriter(String mongoUri, String database, String collection) {
            this.mongoUri = mongoUri;
            this.database = database;
            this.collection = collection;
        }

        @Override
        public void write(Tuple2<Long, List<Score>> element, Context context) {
            if (client == null) {
                client = MongoClients.create(mongoUri);
                MongoDatabase db = client.getDatabase(database);
                col = db.getCollection(collection);
            }

            long ts = element.f0 != null ? element.f0 : 0L;
            List<Score> scores = element.f1;

            Document doc = new Document();
            doc.put("_id", ts);

            // Convert scores to array of documents
            java.util.ArrayList<Document> scoreDocs = new java.util.ArrayList<>();
            if (scores != null) {
                for (Score s : scores) {
                    Map<String, Object> m = new HashMap<>();
                    m.put("userId", s.getId());
                    m.put("score", s.getScore());
                    m.put("lastUpdateTime", s.getLastUpdateTime());
                    scoreDocs.add(new Document(m));
                }
            }
            doc.put("users", scoreDocs);

            // Upsert by _id
            col.replaceOne(new Document("_id", ts), doc, new com.mongodb.client.model.ReplaceOptions().upsert(true));
        }

        @Override
        public void flush(boolean endOfInput) {
            // no-op
        }

        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }
}
