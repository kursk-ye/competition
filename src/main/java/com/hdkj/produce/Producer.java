package com.hdkj.produce;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.AuthorizationFailureException;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.InvalidParameterException;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hdkj.config.GlobalParameter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class Producer {
  private static DatahubClientCreater datahubClientCreater;
  private static final int shardTotal = GlobalParameter.shardTotal;
  private int count = 144;
  private long interval = 1_000L * 60 * 10;

  public void run() throws JsonProcessingException {
    DatahubClient datahubClient = datahubClientCreater.DBC;

    LocalDate today = LocalDate.of(2022, 7, 1);
    LocalTime weeHoursTime = LocalTime.of(0, 0, 0, 0);
    LocalDateTime todayWeeHours = LocalDateTime.of(today, weeHoursTime);
    ZonedDateTime todayZoneDateTime = todayWeeHours.atZone(ZoneId.of("Asia/Shanghai"));
    long eventTS = todayZoneDateTime.toInstant().toEpochMilli();

    int writeShardId = 0;

    for (int i = 0; i <= count; i++) {
      Sign sign = new Sign(eventTS + i * interval,System.currentTimeMillis());
      ObjectMapper objectMapper = new ObjectMapper();
      String strSign = objectMapper.writeValueAsString(sign);

      RecordSchema recordSchema =
          datahubClient
              .getTopic(GlobalParameter.projectName, GlobalParameter.topicName)
              .getRecordSchema();
      TupleRecordData data = new TupleRecordData(recordSchema);
      data.setField("val", strSign);
      RecordEntry recordEntry = new RecordEntry();
      recordEntry.setRecordData(data);
      List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
      recordEntries.add(recordEntry);

      writeShardId = writeShardId >= GlobalParameter.shardTotal ? 0 : writeShardId;

      try {
        datahubClient.putRecordsByShard(
            GlobalParameter.projectName,
            GlobalParameter.topicName,
            String.valueOf(writeShardId),
            recordEntries);
      } catch (InvalidParameterException e) {
        System.out.println("invalid parameter, please check your parameter");
        e.printStackTrace();
      } catch (AuthorizationFailureException e) {
        System.out.println("AK error, please check your accessId and accessKey");
        e.printStackTrace();
      } catch (ResourceNotFoundException e) {
        System.out.println("project or topic or shard not found");
        e.printStackTrace();
      } catch (ShardSealedException e) {
        System.out.println("shard status is CLOSED, can not write");
        e.printStackTrace();
      } catch (DatahubClientException e) {
        System.out.println("other error");
        e.printStackTrace();
      }

      System.out.println("write into datahub :" + strSign);

      writeShardId++;
    }
  }

  public static void main(String[] args) throws JsonProcessingException {
    Producer producer = new Producer();
    producer.run();
  }
}
