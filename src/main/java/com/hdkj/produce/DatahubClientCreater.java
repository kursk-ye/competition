package com.hdkj.produce;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.hdkj.config.GlobalParameter;

public class DatahubClientCreater {
  public final static DatahubClient DBC;

  static  {
    // Endpoint以Region: 华东1为例，其他Region请按实际情况填写
    String endpoint = GlobalParameter.publicEndpoint;
    // String endpoint = GlobalParameter.getVpcEndpoint();
    String accessId = GlobalParameter.accessId;
    String accessKey = GlobalParameter.accessKey;
    // 创建DataHubClient实例
    DBC =
        DatahubClientBuilder.newBuilder()
            .setDatahubConfig(
                new DatahubConfig(
                    endpoint,
                    // 是否开启二进制传输，服务端2.12版本开始支持
                    new AliyunAccount(accessId, accessKey),
                    true))
            // 专有云使用出错尝试将参数设置为           false
            // HttpConfig可不设置，不设置时采用默认值
            .setHttpConfig(
                new HttpConfig()
                    .setCompressType(HttpConfig.CompressType.LZ4) // 读写数据推荐打开网络传输 LZ4压缩
                    .setConnTimeout(10000))
            .build();
  }
}
