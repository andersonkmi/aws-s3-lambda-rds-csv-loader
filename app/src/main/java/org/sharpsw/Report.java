package org.sharpsw;

public class Report {
    private boolean status = false;

    private long fileSize;

    private long executionTime;

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutiongTime(long executiongTime) {
        this.executionTime = executiongTime;
    }
}
