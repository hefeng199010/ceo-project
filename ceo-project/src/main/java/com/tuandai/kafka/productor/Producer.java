package com.tuandai.kafka.productor;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.tuandai.tools.DateFmt;
import com.tuandai.tools.KafkaProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put("client.id", "MyGroup");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }


    /**
     *ceo_screen_topic
     */
    public void run() {
        int i = 0;//业务号
        int j = 0;//标号
        String[] amountArr = { "10","20","50"}; //满标金额
        String date = DateFmt.getCountDate("2018-06-05", DateFmt.date_short);//满标时间
        String[] stateArr =  { "car","house","small"}; //业务类型
        String[] amountOutArr = { "10","20","50"}; //放款金额
        String[] companyArr = { "DG","SZ","GZ"}; //公司
        String[] limitArr = { "1","3","6"}; //期限
        String[] nameArr = { "李白","tom","kasa"}; //姓名
        String[] areaArr = { "GD","BJ","SH"}; //省份
        String[] borrowNameArr = { "李白","tom","kasa"}; //借款人身份
        String[] borrowAmountArr = {  "10","20","50"}; //借款金额
        String[] jobArr = {  "IT","programmer","coder"}; //行业
        Random random = new Random();

        while (true) {
            i++;
            j++;

            String messageStr=i+"\t"+j+"\t"+amountArr[random.nextInt(3)]+"\t"+date+"\t"+date+"\t"+stateArr[random.nextInt(3)]
                    +"\t"+amountOutArr[random.nextInt(3)] +"\t"+companyArr[random.nextInt(3)]+"\t"+limitArr[random.nextInt(3)]
                    +"\t"+nameArr[random.nextInt(3)]+"\t"+areaArr[random.nextInt(3)]+"\t"+borrowNameArr[random.nextInt(3)]
                    +"\t"+borrowAmountArr[random.nextInt(3)]+"\t"+jobArr[random.nextInt(3)];
            try {
                producer.send(new ProducerRecord<>(topic, messageStr)).get();
                System.out.println(messageStr + "=======>"+i);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}

    /**
     *已还本金
     */
   /* public void run() {
        int i = 0;//业务号

        String[] borrowAmountArr = {  "10","20","50"}; //已還本金
        Random random = new Random();

        while (true) {
            i++;
            String messageStr=i+"\t" +borrowAmountArr[random.nextInt(3)];
            try {
                producer.send(new ProducerRecord<>(topic, messageStr)).get();
                System.out.println(messageStr + "=======>"+i);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}*/

class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.err.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Producer producer=new Producer("ceo_screen_topic",false);
//        Producer producer=new Producer("total_business_balance_topic",false);
        producer.start();
    }
}
