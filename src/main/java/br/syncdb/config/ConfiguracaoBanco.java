package br.syncdb.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfiguracaoBanco {

    private static final Properties properties = new Properties();

    static {
        try (InputStream input = ConfiguracaoBanco.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Arquivo application.properties não encontrado.");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Erro ao carregar configurações", e);
        }
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }
}
