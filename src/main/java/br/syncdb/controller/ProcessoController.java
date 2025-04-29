package br.syncdb.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import br.syncdb.service.ProcessoService;

@Controller
public class ProcessoController {
        
    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private ProcessoService processoService;

    @MessageMapping("/ping")
    public void handlePing(Map<String, Object> payload)
    {
        messagingTemplate.convertAndSend("/topic/pong", Map.of(
            "mensagem", "pong",
            "status", "pong"
        ));
    }

    @GetMapping(value = "/processo/cancelar/{base}", produces = "application/json")
    public ResponseEntity<?> cancelarProcesso(@PathVariable (value = "base") String base)
    {
        processoService.cancelarProcesso(base);
        return ResponseEntity.ok().body(Map.of("cancel", true));
    }

}
