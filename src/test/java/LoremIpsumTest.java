import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.util.List;

public class LoremIpsumTest {
    @Rule
    public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());
    @Rule
    public final EnvironmentVariables environmentVariables
            = new EnvironmentVariables();

    @Test
    public void testLoremIpsum(){
        try {
            environmentVariables.set("ZK_QUORUM", kafkaRule.helper().zookeeperConnectionString());
            environmentVariables.set("CONFIG", "{\"config\":{\"length\":\"1\", \"limit\":\"5\"}})");
            environmentVariables.set("CONFIG_BOOTSTRAP_SERVERS", "localhost:"+kafkaRule.helper().kafkaPort());
            new Thread(new LoremIpsum()).start();
            List<String> result = kafkaRule.helper().consumeStrings("lorem", 5).get();
            Assert.assertTrue(!result.isEmpty());
        }catch(Exception e){
            e.printStackTrace();
            Assert.fail();
        }
    }
}
