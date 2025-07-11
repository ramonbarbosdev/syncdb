package br.syncdb.utils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.json.JSONObject;

public class LeitorConfigSegura {
    
 public static Properties carregarConfiguracao(String caminho, String segredo) throws Exception {
        byte[] chave = CriptoUtils.gerarChave256(segredo);

        String conteudo = Files.readString(Path.of(caminho));
        String json = CriptoUtils.descriptografar(conteudo, chave);

        JSONObject obj = new JSONObject(json);
        Properties props = new Properties();
        props.put("user", obj.getString("user"));
        props.put("password", obj.getString("password"));
        props.put("host", obj.getString("host"));
        props.put("database", obj.getString("database"));
        return props;
    }

}
