package com.aotain.collect.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 采集sftp的日志
 */
public class TailFileSFtpSource extends AbstractSource implements Configurable,
        PollableSource {
    private Logger LOG = LoggerFactory.getLogger(TailFileSFtpSource.class);
    private List<Event> eventList = new ArrayList<Event>();

    private String logFilePath = "";
    private String fileNamePrefix = "sftp-server.log";
    private String idleTime = "3000";
    private String hiddenFileName = "_file_sftp_collect_map";
    private static int count=0;
    private static Long time =new Date().getTime();

    private String batchSize = "";
    private Map<String,String> fileCountsMap = Maps.newConcurrentMap();

    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

    @Override
    public void configure(Context context) {
        logFilePath = context.getString("logFilePath");
        Preconditions.checkNotNull(logFilePath, "logFilePath must be set!!");
        fileNamePrefix = context.getString("fileNamePrefix");
        Preconditions.checkNotNull(fileNamePrefix, "fileNamePrefix must be set!!");
        batchSize = context.getString("batchSize");
        Preconditions.checkNotNull(batchSize, "batchSize must be set!!");
        idleTime = context.getString("idleTime");
        idleTime=idleTime==null||(idleTime!=null&&idleTime.equals(""))?3000+"":idleTime;
        LOG.info(" config parameters logFilePath={" + logFilePath + "},fileNamePrefix={" + fileNamePrefix + "}");
        LOG.info(" config parameters batchSize={" + batchSize + "},");
    }

    @Override
    public synchronized void start() {
        LOG.info("************ flume start ***************");
        try {
            File logFile = new File(logFilePath,hiddenFileName);
            final RandomAccessFile randomFile = new RandomAccessFile(logFile, "rw");
            String num ="";
            while((num = randomFile.readLine())!=null){
                if(num!=null&&num.trim()!=""){
                    String[] arr = num.split("\\|");
                    LOG.debug("init map. fileName={"+arr[0]+"},lastTimeFileSize={"+arr[1]+"}");
                    fileCountsMap.put(arr[0],arr[1]);
                }
            }
        } catch (Exception e) {
            LOG.error(" flume source start error,", e);
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        int number=0;
        try {
            Map<String, String> headers;
            byte[] kafkaMessage;
            Event event;
            headers = new HashMap<String, String>();
            headers.put("timestamp", String.valueOf(System.currentTimeMillis()));

            File fileDir = new File(logFilePath);
            if(fileDir.exists()){
                File[] files = fileDir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if(name.contains(fileNamePrefix)){
                            return true;
                        }
                        return false;
                    }
                });
                if(files.length>0){
                    for(File logFile:files){
                        String fileName = logFile.getName();
                        if(fileName.length()<=16){
                            fileName=fileName+"-"+format.format(new Date());
                        }
                        final RandomAccessFile randomFile = new RandomAccessFile(logFile, "rw");
                        Long currentSize=randomFile.length();
                        //这个文件上次就存在了。
                        if(fileCountsMap.containsKey(fileName)){
                            String lastTimeFileSize = fileCountsMap.get(fileName);
                            LOG.debug("file={"+fileName+"} exist. lastTimeFileSize={"+lastTimeFileSize+"},currentSize={"+currentSize+"}");
                            //获得变化部分的
                            randomFile.seek(Long.valueOf(lastTimeFileSize));
                            String tmp = "";
                            while ((tmp = randomFile.readLine()) != null) {
                                count++;
                                LOG.debug("count="+count);
                                if (tmp.contains("End upload into file") && !tmp.contains(".ok")) {
                                    headers.put("logType", "filemanagerlog");
                                    LOG.debug("source get message="+tmp);
                                    kafkaMessage = tmp.getBytes();
                                    event = EventBuilder.withBody(kafkaMessage, headers);
                                    eventList.add(event);
                                    number++;
                                    if(eventList.size()>=Integer.valueOf(batchSize)){
                                        getChannelProcessor().processEventBatch(eventList);
                                        eventList = new ArrayList<Event>();
                                    }
                                }
                            }
                            if (eventList.size() > 0) {
                                getChannelProcessor().processEventBatch(eventList);
                                eventList = new ArrayList<Event>();
                            }
                            lastTimeFileSize = currentSize+"";
                            randomFile.close();
                            fileCountsMap.put(fileName,lastTimeFileSize);
                            status = Status.READY;
                            //新增文件。
                        }else{
                            LOG.debug("file={"+fileName+"} is new create. currentSize={"+currentSize+"}");
                            String lastTimeFileSize = "";
                            randomFile.seek(0);
                            String tmp = "";
                            while ((tmp = randomFile.readLine()) != null) {
                                count++;
                                if (tmp.contains("End upload into file") && !tmp.contains(".ok")) {
                                    headers.put("logType", "filemanagerlog");
                                    LOG.debug("source get message="+tmp);
                                    kafkaMessage = tmp.getBytes();
                                    event = EventBuilder.withBody(kafkaMessage, headers);
                                    eventList.add(event);
                                    number++;
                                    if(eventList.size()>=Integer.valueOf(batchSize)){
                                        getChannelProcessor().processEventBatch(eventList);
                                        eventList = new ArrayList<Event>();
                                    }
                                }
                            }
                            if (eventList.size() > 0) {
                                getChannelProcessor().processEventBatch(eventList);
                                eventList = new ArrayList<Event>();
                            }
                            lastTimeFileSize = randomFile.length()+"";
                            randomFile.close();
                            fileCountsMap.put(fileName,lastTimeFileSize);
                            status = Status.READY;
                        }

                    }
                }
            }
        } catch (Exception e) {
            LOG.error(" process error",e);
        }catch (Throwable t) {
            LOG.error(" process error ", t);
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error) t;
            }
        }


        try {
            if(number == 0){
                Thread.sleep(Long.valueOf(idleTime));
            }
            Long t2  = new Date().getTime();
            if(t2-300000>=time){
                //写文件
                writeMapToDisk();
                time=t2;
            }
            if(count>=500){
                writeMapToDisk();
                count=0;
            }
        } catch (Exception e) {
            LOG.error(" scheduler to disk error",e);
        }

        return status;
    }

    @Override
    public synchronized void stop() {
        LOG.debug("************ flume stop ***************");
        super.stop();
        try {
            writeMapToDisk();
        } catch (Exception e){
            LOG.error(" stop  error ", e);
        } finally {
        }
    }

    private void writeMapToDisk(){
        try {
            File logFile = new File(logFilePath, hiddenFileName);
            final RandomAccessFile randomFile = new RandomAccessFile(logFile, "rw");
            for (Map.Entry<String,String> entry:fileCountsMap.entrySet()){
                String fileName = entry.getKey();
                String fileLength = entry.getValue();
                StringBuilder builder = new StringBuilder();
                builder.append(fileName).append("|").append(fileLength).append("\n");
                randomFile.writeBytes(builder.toString());
            }
            randomFile.close();
        } catch (Exception e) {
            LOG.error("writeMapToDisk error,",e);
        }
    }

    //@Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    //@Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
