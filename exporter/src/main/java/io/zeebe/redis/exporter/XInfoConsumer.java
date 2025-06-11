package io.zeebe.redis.exporter;

import java.nio.charset.Charset;
import java.util.ArrayList;

public class XInfoConsumer {

  private static final Charset charset;

  private String name;

  private long pending = 0;

  private long idle = 0;

  static {
    charset = Charset.forName("UTF-8");
  }

  public static XInfoConsumer fromXInfo(Object xinfoGroup, boolean useProtoBuf) {
    ArrayList<Object> info = (ArrayList<Object>) xinfoGroup;
    String name = null;
    String pending = null;
    String idle = null;
    for (int i = 0; i < info.size(); i++) {
      String current = getValueAt(info, i, useProtoBuf);
      if ("name".equals(current)) {
        name = getValueAt(info, i + 1, useProtoBuf);
      } else if ("pending".equals(current)) {
        pending = getValueAt(info, i + 1, useProtoBuf);
      } else if ("idle".equals(current)) {
        idle = getValueAt(info, i + 1, useProtoBuf);
      }
      if (name != null && idle != null) break;
    }
    return new XInfoConsumer(name, pending, idle);
  }

  private static String getValueAt(ArrayList<Object> list, int i, boolean useProtoBuf) {
    Object obj = list.get(i);
    if (useProtoBuf && obj instanceof byte[]) return new String((byte[]) obj, charset);
    return String.valueOf(obj);
  }

  public XInfoConsumer(String name, String pending, String idle) {
    this.name = name;
    this.pending = pending != null ? Long.parseLong(pending) : 0;
    this.idle = idle != null ? Long.parseLong(idle) : 0;
  }

  public String getName() {
    return name;
  }

  public long getPending() {
    return pending;
  }

  public long getIdle() {
    return idle;
  }

  @Override
  public String toString() {
    return "XInfoConsumer{"
        + "name='"
        + name
        + '\''
        + ", pending='"
        + pending
        + '\''
        + ", idle="
        + idle
        + '}';
  }
}
