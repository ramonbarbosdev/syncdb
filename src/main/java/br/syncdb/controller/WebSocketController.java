package br.syncdb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

@Controller
public class WebSocketController  {

 @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @MessageMapping("/send") 
    public void receiveMessage(String message) {
        System.out.println("Recebido: " + message);
        messagingTemplate.convertAndSend("/topic/sync", "Mensagem recebida: " + message);
    }

}