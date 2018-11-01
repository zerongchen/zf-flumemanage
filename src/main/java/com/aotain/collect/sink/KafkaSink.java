package com.aotain.collect.sink;

import com.aotain.collect.util.KafkaProducer;
import com.aotain.collect.util.LogParse;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaSink extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(KafkaSink.class.getName());
    private String metadata_broker_list;
    private String topic;

    private KafkaProducer producer;

    public KafkaSink() {
        LOG.info("KafkaSink start...");
    }

    @Override
    public void configure(Context context) {
        metadata_broker_list = context.getString("metadata.broker.list");
        Preconditions.checkNotNull(metadata_broker_list, "metadata.broker.list must be set!!");
        topic = context.getString("topic");
        Preconditions.checkNotNull(topic, "topic must be set!!");
    }

    @Override
    public void start() {
        LOG.debug("------------------------ KafkaSink start --------------------------");
        super.start();
        try {
            Map<String, Object> conf = Maps.newHashMap();
            conf.put("metadata.broker.list", metadata_broker_list);
            producer = new KafkaProducer(conf);
            LOG.info(" get kafka producer=" + producer);
        } catch (Exception e) {
            LOG.error(" get kafka producer error ", e);
            System.exit(1);
        }

    }

    @Override
    public void stop() {
        LOG.debug("------------------------ KafkaSink stop --------------------------");
        super.stop();

    }

    @Override
    public Status process() throws EventDeliveryException {
        LOG.debug("------------------------ KafkaSink process --------------------------");
        Status result = Status.READY;
        Channel channel = getChannel();

        Transaction transaction = channel.getTransaction();
        Event event;
        String content;
        transaction.begin();
        try {
            event = channel.take();
            if (event != null) {
                Map<String, String> headers = event.getHeaders();
                String fileType = headers.get("fileType");
                content = new String(event.getBody());
                LOG.debug("sink message="+content);
                // sftp的日志解析
                if (content.contains("End upload into file")) {
                    content = LogParse.getValueMessageSFtp(content);
                    LOG.debug("sink sftplog message="+content);
                    producer.producer(topic, content);

                // file_manager的日志解析
                }else if (content.contains("Successed to upload file")){
                    content = LogParse.getValueMessage(content);
                    producer.producer(topic, content);
                    LOG.debug("sink filemanagerlog message="+content);
                }
            }
            transaction.commit();
        } catch (Throwable e) {
            try {
                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been" +
                        "successful.", e2);
            }
            LOG.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            //   Throwables.propagate(e);
        } finally {
            transaction.close();
        }

        return result;
    }
}