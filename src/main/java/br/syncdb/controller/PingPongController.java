package br.syncdb.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

@Controller
public class PingPongController {
        
    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @MessageMapping("/ping")
    public void handlePing(Map<String, Object> payload)
    {
        messagingTemplate.convertAndSend("/topic/pong", Map.of(
            "mensagem", "pong",
            "status", "pong"
        ));
    }

}
