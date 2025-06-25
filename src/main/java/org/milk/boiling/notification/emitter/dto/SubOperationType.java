package org.milk.boiling.notification.emitter.dto;

import org.milk.boiling.notification.emitter.entity.UserSubscriptions;

import java.util.function.Function;

public enum SubOperationType {
    SUBSCRIBE(MilkSubDTO::subMilkSubDTO),
    HEARTBEAT(MilkSubDTO::heartBeatbMilkSubDTO),
    UNSUBSCRIBE(MilkSubDTO::unnsubMilkSubDTO);

    public final Function<UserSubscriptions, MilkSubDTO> userSubscriptionMilkSubDTOFunction;

    SubOperationType(Function<UserSubscriptions, MilkSubDTO> func) {
        this.userSubscriptionMilkSubDTOFunction = func;
    }
}