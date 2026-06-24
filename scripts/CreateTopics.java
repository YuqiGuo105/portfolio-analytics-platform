// One-shot bootstrap: create the 2 Aiven Kafka topics this platform needs.
//   analytics.raw.events    (2 partitions, replication=2, retention=7d)
//   analytics.events.dlq    (2 partitions, replication=2, retention=14d)
//
// Run with (env vars must be set):
//   java -cp "$(find ~/.m2/repository/org/apache/kafka/kafka-clients/3.7.1 -name '*.jar' | head -1):\
//$(find ~/.m2/repository -name 'slf4j-api-*.jar' | head -1):\
//$(find ~/.m2/repository -name 'lz4-java-*.jar' | head -1):\
//$(find ~/.m2/repository -name 'snappy-java-*.jar' | head -1):\
//$(find ~/.m2/repository -name 'zstd-jni-*.jar' | head -1)" scripts/CreateTopics.java
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;

public class CreateTopics {
    public static void main(String[] args) throws Exception {
        String bootstrap = req("KAFKA_BOOTSTRAP");
        String user = req("KAFKA_USER");
        String pass = req("KAFKA_PASSWORD");

        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        p.put("sasl.mechanism", "SCRAM-SHA-256");
        p.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"" + user + "\" password=\"" + pass + "\";");
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60_000);

        try (Admin admin = Admin.create(p)) {
            Set<String> existing = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            System.out.println("Existing topics: " + existing);

            List<NewTopic> wanted = new ArrayList<>();
            wanted.add(newTopic("analytics.raw.events", 2, (short) 2, 7));
            wanted.add(newTopic("analytics.events.dlq", 2, (short) 2, 14));

            List<NewTopic> missing = new ArrayList<>();
            for (NewTopic t : wanted) if (!existing.contains(t.name())) missing.add(t);

            if (missing.isEmpty()) {
                System.out.println("All topics already exist. Nothing to do.");
                return;
            }
            System.out.println("Creating: " + names(missing));
            admin.createTopics(missing).all().get(60, TimeUnit.SECONDS);
            System.out.println("OK. Topics after create: "
                    + admin.listTopics().names().get(30, TimeUnit.SECONDS));
        }
    }

    private static NewTopic newTopic(String name, int parts, short rf, int retDays) {
        NewTopic t = new NewTopic(name, parts, rf);
        Map<String, String> cfg = new HashMap<>();
        cfg.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retDays * 24L * 3600_000L));
        cfg.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        t.configs(cfg);
        return t;
    }

    private static List<String> names(List<NewTopic> ts) {
        List<String> n = new ArrayList<>();
        for (NewTopic t : ts) n.add(t.name());
        return n;
    }

    private static String req(String k) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) throw new IllegalStateException("Missing env: " + k);
        return v;
    }
}
