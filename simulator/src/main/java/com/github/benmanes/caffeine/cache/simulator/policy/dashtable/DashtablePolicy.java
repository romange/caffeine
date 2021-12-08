package com.github.benmanes.caffeine.cache.simulator.policy.dashtable;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

import java.io.IOException;
import java.io.UncheckedIOException;

@Policy.PolicySpec(name = "dashtable.Dashtable")
public class DashtablePolicy implements Policy.KeyOnlyPolicy {
    private final Dashtable dashtable;
    private final PolicyStats policyStats;

    public DashtablePolicy(Config config) {
        this.policyStats = new PolicyStats(name());
        BasicSettings settings = new BasicSettings(config);

        try {
            this.dashtable = new Dashtable();
            this.dashtable.setMaxSize(settings.maximumSize());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void record(long key) {
        try {
            Long value = dashtable.find(key);
            if (value == null) {
                policyStats.recordMiss();
                if (dashtable.insert(key, 1) == Dashtable.InsertStatus.EVICTED) {
                    policyStats.recordEviction();
                }
            } else {
                policyStats.recordHit();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
