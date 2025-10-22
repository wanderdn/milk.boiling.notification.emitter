package org.milk.boiling.notification.emitter.config;

import org.apache.fury.BaseFury;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SerialicationConfiguration {

    @Bean
    public BaseFury fury() {
        var fury = Fury.builder().requireClassRegistration(true).withLanguage(Language.JAVA).withAsyncCompilation(true).buildThreadSafeFury();
        fury.register(MilkBoilingEvent.class);
        return fury;
    }
}
