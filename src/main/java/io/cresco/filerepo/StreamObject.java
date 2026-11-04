package io.cresco.filerepo;

public class StreamObject {

    private long startByte;
    private long byteLength;

    private String transferId;

    private volatile long bytesTransfered = 0;

    private String filename;

    private volatile boolean isActive = true;

    // the open file a transfer reads through, from the moment it is accepted until its streamer closes it
    private volatile java.nio.channels.FileChannel channel;

    void setChannel(java.nio.channels.FileChannel channel) {
        this.channel = channel;
    }

    /** Close the transfer's file if its streamer never ran (shutdown). */
    void closeChannel() {
        java.nio.channels.FileChannel c = channel;
        channel = null;
        if (c != null) {
            try { c.close(); } catch (java.io.IOException ignore) { }
        }
    }

    public StreamObject(String transferId, String filename, long startByte, long byteLength) {
        this.transferId = transferId;
        this.filename = filename;
        this.startByte = startByte;
        this.byteLength = byteLength;
    }

    public long getStartByte() {
        return startByte;
    }

    public void setStartByte(long startByte) {
        this.startByte = startByte;
    }

    public long getByteLength() {
        return byteLength;
    }

    public void setByteLength(long byteLength) {
        this.byteLength = byteLength;
    }

    public String getTransferId() {
        return transferId;
    }

    public void setTransferId(String transferId) {
        this.transferId = transferId;
    }

    public long getBytesTransfered() {
        return bytesTransfered;
    }

    public void setBytesTransfered(long bytesTransfered) {
        this.bytesTransfered = bytesTransfered;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    @Override
    public String toString() {
        return "StreamObject{" +
                "startByte=" + startByte +
                ", byteLength=" + byteLength +
                ", transferId='" + transferId + '\'' +
                ", bytesTransfered=" + bytesTransfered +
                ", filename='" + filename + '\'' +
                ", isActive=" + isActive +
                '}';
    }
}
