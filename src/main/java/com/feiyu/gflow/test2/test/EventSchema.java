package com.feiyu.gflow.test2.test;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

public class EventSchema<Event> implements DeserializationSchema<Event>, SerializationSchema<Event> {

    private static Gson gson;

    public EventSchema() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
        gson = gsonBuilder.create();
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        Type jsonType = new TypeToken<Event>(){}.getType();
        return gson.fromJson(new String(bytes), jsonType);
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public byte[] serialize(Event event) {
        return gson.toJson(event).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of((Class<Event>) new TypeToken<Event>(){}.getRawType());
    }
}
