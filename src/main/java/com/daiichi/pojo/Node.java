package com.daiichi.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Random;

@Data
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class Node {
    private final String id;
    private final String planId;
    private final String data;
    @Builder.Default
    private Node.Status status = Node.Status.Pending;
    private final String parentId;

    public void progressStatus() {
        if (status == Node.Status.Pending)
            status = Node.Status.Running;
        else if (status == Node.Status.Running) {
            status = new Random().nextInt(2) == 0 ? Node.Status.Completed : Node.Status.Error;
        } else if (status == Node.Status.Error) {
            status = Node.Status.Completed;
        }
    }

    public enum Status {
        Pending, Running, Completed, Error
    }
}
