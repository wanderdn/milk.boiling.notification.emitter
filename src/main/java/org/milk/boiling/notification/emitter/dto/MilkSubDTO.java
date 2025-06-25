package org.milk.boiling.notification.emitter.dto;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.milk.boiling.notification.emitter.entity.UserSubscriptions;

public record MilkSubDTO(@JsonUnwrapped UserSubscriptions subscriber,
                         SubOperationType SubOperationType
) {
    public static MilkSubDTO unnsubMilkSubDTO(UserSubscriptions subscriber) {
        return new MilkSubDTO(subscriber, org.milk.boiling.notification.emitter.dto.SubOperationType.UNSUBSCRIBE);
    }

    public static MilkSubDTO subMilkSubDTO(UserSubscriptions subscriber) {
        return new MilkSubDTO(subscriber, org.milk.boiling.notification.emitter.dto.SubOperationType.SUBSCRIBE);
    }

    public static MilkSubDTO heartBeatbMilkSubDTO(UserSubscriptions subscriber) {
        return new MilkSubDTO(subscriber, org.milk.boiling.notification.emitter.dto.SubOperationType.HEARTBEAT);
    }


}



