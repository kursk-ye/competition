package com.hdkj.produce;

import lombok.Data;

@Data
public class Sign {
    private long event_time;
    private long write_time;

    public Sign(long event_time, long write_time) {
        this.event_time = event_time;
        this.write_time = write_time;
    }
}
