package br.syncdb.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfiguracaoBanco {

    private static final Properties properties = new Properties();

    static {
        carregarConfiguracoes();
    }

    private static void carregarConfiguracoes()
    {
	   String perfil = System.getProperty("spring.profiles.active", "dev");
       String nomeArquivo = "application-" + perfil + ".properties";

        try (InputStream input = ConfiguracaoBanco.class.getClassLoader().getResourceAsStream(nomeArquivo)) {
            if (input == null) {
                throw new RuntimeException("Arquivo " + nomeArquivo + " não encontrado.");
            }
            properties.load(input);
            System.out.println("Configuração carregada: " + nomeArquivo);
        } catch (IOException e) {
            throw new RuntimeException("Erro ao carregar configurações do arquivo " + nomeArquivo, e);
        }
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }
}
