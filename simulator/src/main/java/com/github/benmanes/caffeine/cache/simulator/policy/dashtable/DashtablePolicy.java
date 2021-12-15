package com.github.benmanes.caffeine.cache.simulator.policy.dashtable;


import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;
import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.FileWriter;  
import java.io.Writer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;


@Policy.PolicySpec(name = "dashtable.Dashtable", characteristics = WEIGHTED)
public class DashtablePolicy implements Policy {
    private final Dashtable dashtable;
    private final PolicyStats policyStats;
    private final String logName = "/tmp/dash.log";
    Logger logger = System.getLogger(DashtablePolicy.class.getName());
    List<AccessEvent> buffer;
    Writer fileWriter;
    long numRecords = 0;

    public DashtablePolicy(Config config, Set<Characteristic> characteristics) {
        this.policyStats = new PolicyStats(name());
        this.buffer = new ArrayList<AccessEvent>();        
        BasicSettings settings = new BasicSettings(config);

        try {
            this.dashtable = new Dashtable();
            this.dashtable.setMaxSize(settings.maximumSize());
            this.fileWriter = new FileWriter(logName);
            // this.fileWriter.write("MAX " + settings.maximumSize() + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void record(AccessEvent event) {
        // buffer.add(event);
        ++numRecords;
        try {
            fileWriter.write("FINDINSERT " + event.key() + " " + event.weight() + "\n");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (buffer.size() >= 1000) {
            flush();
        }
        /*

        long key = event.key();
        int weight = event.weight();
        
        try {
            Long value = dashtable.find(key);
            if (value == null) {
                policyStats.recordWeightedMiss(weight);
                if (dashtable.insert(key, weight) == Dashtable.InsertStatus.EVICTED) {
                    policyStats.recordEviction();
                }
            } else {
                policyStats.recordWeightedHit(weight);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        */
    }

    @Override
    public void finished() {
      logger.log(Level.INFO, "DashtablePolicy.Finished");
      flush();
      try {
          this.fileWriter.flush();
          this.fileWriter.close();
          dashtable.loadFile(logName, numRecords, policyStats);
      }
      catch (IOException e) {
         throw new UncheckedIOException(e);
      }
    }


    private void flush() {
        try {
            dashtable.insertMany(buffer, policyStats);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buffer.clear();
    }

}
