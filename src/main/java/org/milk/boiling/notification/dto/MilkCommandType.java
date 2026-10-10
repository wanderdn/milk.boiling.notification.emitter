package org.milk.boiling.notification.dto;

public enum MilkCommandType implements DTOInterface {
    PING,
    PONG,
    PAYLOAD,
    SUBSCRIBE,
    HEARTBEAT,
    UNSUBSCRIBE;


}