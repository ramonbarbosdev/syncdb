package br.syncdb.utils;

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
}
