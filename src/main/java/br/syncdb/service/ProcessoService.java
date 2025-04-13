package br.syncdb.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProcessoService {

    private final SimpMessagingTemplate messagingTemplate;

    @Autowired
    public ProcessoService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    
    public void enviarProgresso(String status, int progresso, String mensagem, String tabelaAtual) {
        Map<String, Object> progressoMsg = new HashMap<>();
        progressoMsg.put("status", status);
        progressoMsg.put("progresso", progresso);
        progressoMsg.put("mensagem", mensagem);
        progressoMsg.put("tabelaAtual", tabelaAtual);
        progressoMsg.put("timestamp", System.currentTimeMillis());
        
        messagingTemplate.convertAndSend("/topic/sync/progress", progressoMsg);
    }

}
