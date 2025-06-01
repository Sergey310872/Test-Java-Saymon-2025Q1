package ru.sergey310872.service;

//import org.apache.kafka.clients.consumer.Consumer;

import ru.sergey310872.dto.SinkMessage;

import java.util.function.Consumer;

interface Sink extends Consumer<SinkMessage> {

}
