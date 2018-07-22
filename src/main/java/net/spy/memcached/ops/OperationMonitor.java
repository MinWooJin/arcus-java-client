package net.spy.memcached.ops;

import net.spy.memcached.compat.SpyObject;

import java.net.SocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OperationMonitor extends SpyObject {
  private Date sendTime = null;
  private Date recvTime = null;
  private Date toutTime = null;
  private Operation operation;
  private String src;
  private SocketAddress dest;
  private String keyString;
  private String argumentsString;
  private OperationMonitorWriter operationMonitorWriter;


  private DateFormat sdfr = new SimpleDateFormat("HH:mm:ss.SSS");

  public OperationMonitor(OperationMonitorWriter omw, Operation op, String host, SocketAddress ds, String key, String arguments) {
    super();
    operationMonitorWriter = omw;
    sendTime = new Date();
    operation = op;
    src = host;
    dest = ds;
    keyString = (key != null ? (key.length() > 250 ? key.substring(0, 250) : key) : null);
    argumentsString = arguments;
  }

  public void putOperationMonitor() {
    setRecvTime();
    operationMonitorWriter.putOpertionMonitor(this);
  }

  public void setRecvTime() {
    recvTime = new Date();
  }

  public void setToutTime() {
    toutTime = new Date();
  }

  public void write() {
    getLogger().warn("[ARCUS::MONITOR]" + toString());
  }

  public String toString() {
    return "{STime=" + sdfr.format(sendTime) + ", RTime=" + sdfr.format(recvTime)
            + ", latency=" + (recvTime != null ? (recvTime.getTime()-sendTime.getTime()) : "null") + " msec"
            + ", OTime=" + (toutTime != null ? sdfr.format(toutTime) : "null")
            + ", src=" + src
            + ", dest=" + dest
            + ", op=" + operation
            + ", key=" + keyString
            + ", arguments=" + argumentsString + "}";
  }
}
