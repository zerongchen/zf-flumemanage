package com.aotain.collect.util;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;


/**
 * kafka生产者
 * 
 * @author Administrator
 */
public class KafkaProducer {

    /**
     * log4j日志
     */
    public Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    /**
     * 参数, map中需要的key: metadata.broker.list
     */
    protected Map<String, Object> conf = new HashMap<String, Object>();
    
    /**
     * 构造函数
     * @param conf
     */
    public KafkaProducer(Map<String, Object> conf) {
        // TODO Auto-generated constructor stub
        this.conf = conf;
    }

    /**
     * 发送单条消息
     * @param message
     * @return
     */
    public boolean producer(String topic, String message) {
        // TODO Auto-generated method stub

        Producer<String, String> _producer = openProducer();

        KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, message);
        // message可以带key, 根据key来将消息分配到指定区, 如果没有key则随机分配到某个区
        // KeyedMessage<Integer, String> keyedMessage = new KeyedMessage<Integer, String>("test",
        // 1, message);
        _producer.send(keyedMessage);

        closeProducer(_producer);

        return true;
    }

    /**
     * 发送多条消息
     * @param message
     * @return
     */
    public boolean producer(String topic, List<String> message) {
        // TODO Auto-generated method stub

        Producer<String, String> _producer = openProducer();

        List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
        for (String msg : message) {
            messages.add(new KeyedMessage<String, String>(topic, msg));
        }
        _producer.send(messages);

        closeProducer(_producer);

        return true;
    }

    /**
     * 打开生产者
     * @return 生产者
     */
    private Producer<String, String> openProducer() {
        Producer<String, String> _producer = null;

        Properties _props = new Properties();
        // 消息传递到broker时的序列化方式
        _props.put("serializer.class", StringEncoder.class.getName());
        for(Entry<String, Object> entry : conf.entrySet()){
        
            _props.put(entry.getKey(), entry.getValue());
        }
        _producer = new Producer<String, String>(new ProducerConfig(_props));

        return _producer;
    }

    /**
     * 关闭生产者
     * @param _producer 生产者
     */
    private void closeProducer(Producer<String, String> _producer) {
        if (_producer != null) _producer.close();
    }
}
