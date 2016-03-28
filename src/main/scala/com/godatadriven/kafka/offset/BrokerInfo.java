package com.godatadriven.kafka.offset;

import com.google.gson.annotations.SerializedName;
import org.apache.kafka.common.protocol.SecurityProtocol;

import java.net.URI;
import java.net.URISyntaxException;

public class BrokerInfo {
  @SerializedName("endpoints")
  private String[] endpoints;

  @SerializedName("host")
  private String host;

  @SerializedName("port")
  private int port;

  public String getHost() {
    if (host != null && !host.isEmpty()) {
      return host;
    } else if (endpoints != null && endpoints.length > 0) {
      try {
        URI uri = new URI(endpoints[0]);
        return uri.getHost();
      } catch (URISyntaxException e) {
        return null;
      }
    }
    return null;
  }

  public int getPort() {
    if (port != -1) {
      return port;
    } else if (endpoints != null && endpoints.length > 0) {
      try {
        URI uri = new URI(endpoints[0]);
        return uri.getPort();
      } catch (URISyntaxException e) {
        return -1;
      }
    }
    return -1;
  }

  public SecurityProtocol getSecurityProtocol() {
    if (endpoints != null && endpoints.length > 0) {
      try {
        URI uri = new URI(endpoints[0]);
        if (uri.getScheme().toUpperCase().equals(SecurityProtocol.PLAINTEXTSASL.name)) {
          return SecurityProtocol.PLAINTEXTSASL;
        } else if (uri.getScheme().toUpperCase().equals(SecurityProtocol.SASL_PLAINTEXT.name)) {
          return SecurityProtocol.SASL_PLAINTEXT;
        }
      } catch (URISyntaxException e) {

      }
    }
    return SecurityProtocol.PLAINTEXT;
  }
}
