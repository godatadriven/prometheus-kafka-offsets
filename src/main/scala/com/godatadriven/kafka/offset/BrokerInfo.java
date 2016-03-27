package com.godatadriven.kafka.offset;

import com.google.gson.annotations.SerializedName;

public class BrokerInfo {
  @SerializedName("endpoints")
  public String[] endpoints;

  @SerializedName("host")
  public String host;
  @SerializedName("port")
  public int port;

  public String getEndpoint() {
    return endpoints[0];
  }
}
