package com.github.benmanes.caffeine.cache.simulator.policy.dashtable;

import com.google.common.util.concurrent.UncheckedExecutionException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import java.util.List;
import java.util.ArrayList;

public class Dashtable implements AutoCloseable {
    private final Socket socket;
    private final BufferedReader reader;
    private final PrintWriter writer;

    public enum InsertStatus {
        GOOD,
        BAD,
        EVICTED
    }

    static class BatchResult {
       public int evicted = 0;
       public List<Integer> misses = new ArrayList<Integer>();

       public List<Integer> hits = new ArrayList<Integer>();

    } 

    public Dashtable() throws IOException {
        socket = new Socket("localhost", 41998);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new PrintWriter(socket.getOutputStream(), true);
    }

    public void setMaxSize(long max) throws IOException {
        writer.write("MAX " + max + "\n");
        writer.flush();
        reader.readLine();
    }

    public InsertStatus insert(long key, long value) throws IOException {
        writer.write("INSERT " + key + " " + value + "\n");
        writer.flush();
        return InsertStatus.valueOf(reader.readLine());
    }

    public Long find(long key) throws IOException {
        writer.write("FIND " + key + "\n");
        writer.flush();
        String resp = reader.readLine();
        if (resp.equals("BAD")) {
            return null;
        }

        return Long.parseLong(resp);
    }

    public void insertMany(List<AccessEvent> list, PolicyStats stats) throws IOException {        
        for (AccessEvent event : list) {
           writer.write("FINDINSERT " + event.key() + " " + event.weight() + "\n");
        }
        writer.flush();        
        for (AccessEvent event : list) {            
           String resp = reader.readLine();
           recordStats(resp, event.weight(), stats);
        }
    }

    public void loadFile(String fileName, long numRecords, PolicyStats stats) throws IOException {
        writer.write("LOAD " + fileName + "\n");
        writer.flush();
        for (long i = 0; i < numRecords; ++i) {
           String resp = reader.readLine();
           recordStats(resp, 1, stats);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
            writer.close();
            socket.close();
        } catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    private void recordStats(String resp, int weight, PolicyStats stats) {
        if (resp.equals("EVICTED")) {
            stats.recordEviction();
            stats.recordWeightedMiss(weight);
        } else if (resp.equals("MISS")) {
            stats.recordWeightedMiss(weight);
        } else if (resp.equals("HIT")) {
            stats.recordWeightedHit(weight);               
        } else {
            throw new IllegalStateException();  
        }            
    }
}
