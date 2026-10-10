package org.milk.boiling.notification.dto;

import com.fasterxml.jackson.annotation.JsonUnwrapped;


import static org.milk.boiling.notification.dto.MilkCommandType.SUBSCRIBE;
import static org.milk.boiling.notification.dto.MilkCommandType.UNSUBSCRIBE;


public record MilkSubDTO(@JsonUnwrapped UserSubscriptions subscriber,
                         MilkCommandType MilkCommandType) implements DTOInterface {
    public static MilkSubDTO unnsubMilkSubDTO(UserSubscriptions subscriber) {
        return new MilkSubDTO(subscriber, UNSUBSCRIBE);
    }

    public static MilkSubDTO subMilkSubDTO(UserSubscriptions subscriber) {
        return new MilkSubDTO(subscriber, SUBSCRIBE);
    }


}



