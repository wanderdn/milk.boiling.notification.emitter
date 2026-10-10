package org.milk.boiling.notification.dto;

public interface DTOInterface {

    default public <T> T getPayload() {
        return (T) this;
    }
}
