package com.example.pocsqsspring;

import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import jakarta.jms.JMSException;
import lombok.extern.log4j.Log4j2;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class Listener {

    @JmsListener(destination = "TestQueuePablo")
    public void receiveMessage(SQSTextMessage message) throws JMSException {
        log.info("Received message: {}", message.getText());
    }

}
