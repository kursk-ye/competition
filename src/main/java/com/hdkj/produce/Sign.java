package com.hdkj.produce;

import lombok.Data;

@Data
public class Sign {
    private long event_time;

    public Sign(long event_time) {
        this.event_time = event_time;
    }
}
