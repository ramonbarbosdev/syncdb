package br.syncdb.service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProcessoService {

    private final SimpMessagingTemplate messagingTemplate;

    private final Map<String, AtomicBoolean> processosCancelados = new ConcurrentHashMap<>();

    public void iniciarProcesso(String processoId) {
        processosCancelados.put(processoId, new AtomicBoolean(false));
    }

    public void cancelarProcesso(String processoId) {
        processosCancelados.computeIfPresent(processoId, (id, flag) -> {
            flag.set(true);
            return flag;
        });
    }

    public boolean isCancelado(String processoId) {
        return processosCancelados.getOrDefault(processoId, new AtomicBoolean(false)).get();
    }

    public void finalizarProcesso(String processoId) {
        processosCancelados.remove(processoId);
    }


    @Autowired
    public ProcessoService(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }
    
    public void enviarProgresso(String status, int progresso, String mensagem, String tabelaAtual)
    {

        Map<String, Object> progressoMsg = new HashMap<>();
        progressoMsg.put("status", status);
        progressoMsg.put("progresso", progresso);
        progressoMsg.put("mensagem", mensagem);
        progressoMsg.put("tabelaAtual", tabelaAtual);
        progressoMsg.put("timestamp", System.currentTimeMillis());

        messagingTemplate.convertAndSend("/topic/sync/progress", progressoMsg);
    }
   
}
