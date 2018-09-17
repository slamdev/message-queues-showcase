package com.github.slamdev.mq;

import java.util.Objects;

public class Message {

    private final String messageBody;
    private final String receiptHandle;

    public Message(String messageBody, String receiptHandle) {
        this.messageBody = messageBody;
        this.receiptHandle = receiptHandle;
    }

    public String getMessageBody() {
        return messageBody;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(messageBody, message.messageBody) &&
                Objects.equals(receiptHandle, message.receiptHandle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageBody, receiptHandle);
    }

    @Override
    public String toString() {
        return "{"
                + "messageBody='" + messageBody + '\''
                + ", receiptHandle='" + receiptHandle + '\''
                + '}';
    }
}
