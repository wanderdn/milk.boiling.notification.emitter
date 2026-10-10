package org.milk.boiling.notification.emitter.service.websocket;

import org.milk.boiling.notification.dto.MilkSubDTO;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.UserSubscriptions;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.milk.boiling.notification.emitter.service.RemoteUserSubscriptionService;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Sinks;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.UUID;

@Service
//@ConditionalOnProperty(value = "app.sub.transport", havingValue = "websocket")
public class UserWebSocketSubscriptionService implements RemoteUserSubscriptionService {


    private final Sinks.Many<Object> sendMessageSink;

    private final String hostName;


    public UserWebSocketSubscriptionService(Sinks.Many<Object> sendMessageSink) throws UnknownHostException {
        this.sendMessageSink = sendMessageSink;
        hostName = Inet4Address.getLocalHost().getHostName();
    }

    @Override
    public void removeSubscription(UUID userId) {
        sendMessageSink.tryEmitNext(new UserWebSocketCommand(MilkCommandType.UNSUBSCRIBE, MilkSubDTO.unnsubMilkSubDTO(UserSubscriptions.createSubscription(userId))));
    }

    @Override
    public void createSubscription(UUID userId) {
        sendMessageSink.tryEmitNext(new UserWebSocketCommand(MilkCommandType.SUBSCRIBE, MilkSubDTO.subMilkSubDTO(UserSubscriptions.createSubscription(userId))));
    }
}
