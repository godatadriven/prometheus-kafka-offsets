package com.godatadriven.kafka.offset;

import com.google.gson.annotations.SerializedName;

public class PartitionInfo {
  @SerializedName("leader")
  public String leader;
}
