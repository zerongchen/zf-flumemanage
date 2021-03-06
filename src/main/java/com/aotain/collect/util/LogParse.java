package com.aotain.collect.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParse {
    private static Logger LOG = LoggerFactory.getLogger(LogParse.class.getName());
    private static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
    private static SimpleDateFormat format_2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getValueMessageSFtp(String line) {

        try {
            Map<String, String> sftpMap = new HashMap<String, String>();
            String ip = null;
            String regEx = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(line);

            if (m.find()) {
                String result = m.group();
                ip = result;
            }
            int fileNameStartIndex = line.indexOf("0x01");
            int fileNameEndIndex = line.indexOf(".gz");
            String fileName = line.substring(fileNameStartIndex, fileNameEndIndex + 3);

            int fileSizeStartIndex = line.indexOf("(");
            int fileSizeEndIndex = line.indexOf(")");
            String fileSize = line.substring(fileSizeStartIndex + 1, fileSizeEndIndex - 6);

            String filereceivedtime = line.substring(0, 19);

            String[] f = fileName.split("\\+");
            // 302 接收文件信息datamessage格式定义：
            sftpMap.put("filename", fileName);
            sftpMap.put("filereceivedtime", format_2.parse(filereceivedtime).getTime() / 1000 + "");
            sftpMap.put("filecreatetime", (format.parse(f[6]).getTime() / 1000) + "");
            sftpMap.put("filesize", fileSize);
            sftpMap.put("dpi_ip", ip);

            Map<String, Object> subMap = new HashMap<String, Object>();
            subMap.put("datatype", 3);
            subMap.put("datasubtype", 302);
            subMap.put("datamessage", sftpMap);

            Map<String, Object> map = new HashMap<String, Object>();
            map.put("type", 4);
            map.put("message", subMap);
            map.put("createtime", System.currentTimeMillis() / 1000);
            map.put("createip", getHostAddressAndIp());
            ObjectMapper mapper = new ObjectMapper();

            return mapper.writeValueAsString(map);
        } catch (Exception e) {
            LOG.error("parse message={" + line + "}, error ", e);
        }
        return "";
    }

    public static String getValueMessage(String line) {
        try {

            String ip = null;
            String regEx = "((2[0-4]\\d|25[0-5]|[01]?\\d\\d?)\\.){3}(2[0-4]\\d|25[0-5]|[01]?\\d\\d?)";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(line);

            if (m.find()) {
                String result = m.group();
                ip = result;
            }

            Map<String, String> sftpMap = new HashMap<String, String>();

            int fileNameStartIndex = line.indexOf("file[");
            int fileNameEndIndex = line.lastIndexOf("tar.gz");
            String fileName = line.substring(fileNameStartIndex+5, fileNameEndIndex+6);

            int fileSizeStartIndex = line.indexOf("(");
            int fileSizeEndIndex = line.indexOf(")");
            String fileSize = line.substring(fileSizeStartIndex + 1, fileSizeEndIndex);

            String fileuploadtime = line.substring(0, 19);
            try {
                sftpMap.put("fileuploadtime", (format_2.parse(fileuploadtime).getTime()/1000)+"");
            } catch (ParseException e) {
                LOG.error("line={"+line+"},date parse error,",e);
            }
            sftpMap.put("filename", fileName);
            sftpMap.put("filesize", fileSize);
            // 暂时得不到这个字段信息
            sftpMap.put("received_ip", ip);

            Map<String, Object> subMap = new HashMap<String, Object>();
            subMap.put("datatype", 3);
            subMap.put("datasubtype", 303);
            subMap.put("datamessage", sftpMap);

            Map<String, Object> map = new HashMap<String, Object>();
            map.put("type", 4);
            map.put("message", subMap);
            map.put("createtime", System.currentTimeMillis() / 1000);
            map.put("createip", getHostAddressAndIp());
            ObjectMapper mapper = new ObjectMapper();

            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            LOG.error("parse message={" + line + "}, error ", e);
        }

        return null;
    }

    /**
     * 获取服务器名+IP
     *
     * @return
     */
    private static String getHostAddressAndIp() {
        String ip = "";
        try {
           ip = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (Exception e) {
            LOG.error("get host address error!", e);
        }
        return ip;
    }

    public static void main(String[] args) {
       String g = LogParse.getValueMessage("2018-05-02 16:41:27 [INFO]：UpLoadFileProc[Task: 2 1]: Successed to upload file[0x01+0x0300+001+M-GD-SZ+AOT+001+20180502160508.tar.gz], size(12868151) to serverip[192.168.50.159].\n");
        System.out.println(g);
    }
}
