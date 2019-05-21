/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.infai.seits.sepl.operators.*;
import org.json.JSONObject;

import java.util.Properties;


public class LoremIpsum implements Runnable{

    private String[] lorem;
    private int length, offset, limit;
    private Builder builder;
    private KafkaProducer producer;
    private String topic;

    public LoremIpsum(){
        length = Integer.parseInt(new Config().getConfigValue("length", "1"));
        limit = Integer.parseInt(new Config().getConfigValue("limit", "2"));
        lorem = TextProvider.getSentences();
        builder = new Builder(Helper.getEnv("OPERATOR_ID", "LoremIpsum"), Helper.getEnv("PIPELINE_ID", "LoremIpsumPipeline"));
        Properties config = Stream.config();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer(config);
        topic = Helper.getEnv("OUTPUT", "lorem");
    }

    @Override
    public void run() {
        Message message;
        for(int j=0; j<limit; j++) {
            String output = "";
            for (int i = 0; i < length; i++) {
                output += lorem[i + offset] + " ";
                offset++;
                if (offset == lorem.length) {
                    offset = 0;
                }
            }
            message = new Message(builder.formatMessage("{}"));
            message.output("sentence", output);

            producer.send(new ProducerRecord<String, String>(topic, message.getMessageString()));
        }
    }
}
