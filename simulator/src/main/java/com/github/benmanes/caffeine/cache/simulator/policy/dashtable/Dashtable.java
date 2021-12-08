package com.github.benmanes.caffeine.cache.simulator.policy.dashtable;

import com.google.common.util.concurrent.UncheckedExecutionException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Dashtable implements AutoCloseable {
    private final Socket socket;
    private final BufferedReader reader;
    private final PrintWriter writer;

    public enum InsertStatus {
        GOOD,
        BAD,
        EVICTED
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
}
