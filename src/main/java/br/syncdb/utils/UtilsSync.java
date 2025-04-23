package br.syncdb.utils;

import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class UtilsSync {
    

    public String extrairSchema(String nomeTabela)
    {
        if (nomeTabela.contains("."))  return nomeTabela.split("\\.")[0];
        return null; 
    }
    public String extrairTabela(String nomeTabela)
    {
        if (nomeTabela.contains("."))  return nomeTabela.split("\\.")[1];
        return null; 
    }

    public void  tratarErroSincronizacao(Map<String, Object> response, Exception e)
    { 
        System.out.println("Erro durante sincronização: " + e.getMessage());       
        String errorType = e.getClass().getSimpleName();
        String details = e.getMessage();

        response.put("sucesso", false);
        response.put("erro",errorType);
        response.put("mensagem", "Erro durante sincronização");
        response.put("detalhes", details);
    }
    
}
