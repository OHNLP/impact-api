package org.ohnlp.cat.api.jobs;

import java.util.HashMap;
import java.util.Map;

public enum JobStatus {
    QUEUED(0),
    PREFLIGHT(1),
    IN_PROGRESS(2),
    COMPLETE(3),
    ERROR(-1),
    CANCELED(-2);

    private final int code;
    private static Map<Integer, JobStatus> codeToStatus;

    static {
        codeToStatus = new HashMap<>();
        for (JobStatus status : JobStatus.values()) {
            codeToStatus.put(status.code, status);
        }
    }

    public static JobStatus forCode(int i) {
        return codeToStatus.get(i);
    }

    JobStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

}
