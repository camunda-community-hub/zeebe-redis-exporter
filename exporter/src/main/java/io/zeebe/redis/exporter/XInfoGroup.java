package io.zeebe.redis.exporter;

import java.nio.charset.Charset;
import java.util.ArrayList;

public class XInfoGroup {

    private static final Charset charset;

    private String name;
    private long lastDeliveredId;

    private long pending = 0;

    static {
        charset = Charset.forName("UTF-8");
    }

    public static XInfoGroup fromXInfo(Object xinfoGroup, boolean useProtoBuf) {
        ArrayList<Object> info = (ArrayList<Object>) xinfoGroup;
        String name = null;
        String lastDeliveredId = null;
        String pending = null;
        for (int i = 0; i < info.size(); i++) {
            String current = getValueAt(info, i, useProtoBuf);
            if ("name".equals(current)) {
                name = getValueAt(info, i+1, useProtoBuf);
            } else if ("last-delivered-id".equals(current)) {
                lastDeliveredId = getValueAt(info, i+1, useProtoBuf);
            } else if ("pending".equals(current)) {
                pending = getValueAt(info, i+1, useProtoBuf);
            }
            if (name != null && lastDeliveredId != null) break;
        }
        return new XInfoGroup(name, lastDeliveredId, pending);
    }

    private static String getValueAt(ArrayList<Object> list, int i, boolean useProtoBuf) {
        Object obj = list.get(i);
        if (useProtoBuf && obj instanceof byte[])
            return new String((byte[]) obj, charset);
        return String.valueOf(obj);
    }

    public XInfoGroup(String name, String lastDeliveredId, String pending) {
        this.name = name;
        this.lastDeliveredId = getMessageIdAsLong(lastDeliveredId);
        this.pending = pending != null ? Long.parseLong(pending) : 0;
    }

    public String getName() {
        return name;
    }

    public void considerPendingMessageId(String pendingMessageId) {
        if (pendingMessageId == null) return;
        long pendingMessageIdLongVal = getMessageIdAsLong(pendingMessageId);
        if (pendingMessageIdLongVal < lastDeliveredId || lastDeliveredId == 0) {
            lastDeliveredId = pendingMessageIdLongVal;
        }
    }

    public long getLastDeliveredId() {
        return lastDeliveredId;
    }

    public long getPending() {
        return pending;
    }

    private long getMessageIdAsLong(String messageId) {
        if (messageId == null) return 0;
        int idx = messageId.lastIndexOf('-');
        if (idx < 0) return Long.parseLong(messageId);
        return Long.parseLong(messageId.substring(0, idx));
    }

    @Override
    public String toString() {
        return "XInfoGroup{" +
                "name='" + name + '\'' +
                ", lastDeliveredId='" + lastDeliveredId + '\'' +
                ", pending=" + pending +
                '}';
    }
}
