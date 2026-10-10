package org.milk.boiling.notification.dto;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import lombok.extern.slf4j.Slf4j;
import org.apache.fury.BaseFury;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class SerializationService {
    private final ClassInfoList.ClassInfoFilter isDtoInterace = x -> x.implementsInterface(DTOInterface.class);
    private final BaseFury baseFury;

    public SerializationService() {
        baseFury = Fury.builder().requireClassRegistration(false).withLanguage(Language.JAVA).withAsyncCompilation(true).buildThreadSafeFury();
        try (var cg = new ClassGraph().acceptPackages("org.milk.boiling.notification.dto").scan()) {
            cg.getAllClasses()
                    .filter(isDtoInterace)
                    .forEach(furyClass -> baseFury.register(furyClass.getClass()));
        }
    }

    public byte[] serialize(Object object) {
        return baseFury.serialize(object);
    }

    public <T> T deserialize(byte[] bytea) {
        return (T) baseFury.deserialize(bytea);
    }
}
