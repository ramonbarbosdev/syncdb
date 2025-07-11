package br.syncdb.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigPropertiesBanco {

    private static final Properties properties = new Properties();

    static {
        carregarConfiguracoes();
    }

    private static void carregarConfiguracoes()
    {
        String perfil = "dev"; 

        try (InputStream baseInput = ConfigPropertiesBanco.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (baseInput != null)
            {
                Properties baseProps = new Properties();
                baseProps.load(baseInput);

                String perfilDetectado = baseProps.getProperty("spring.profiles.active");
                if (perfilDetectado != null && !perfilDetectado.isBlank())
                {
                    perfil = perfilDetectado;
                }

                System.out.println("Perfil ativo detectado: " + perfil);
            }
            else
            {
                System.out.println("Arquivo application.properties não encontrado. Usando perfil padrão: " + perfil);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Erro ao carregar application.properties", e);
        }

        String nomeArquivo = "application-" + perfil + ".properties";
        try (InputStream input = ConfigPropertiesBanco.class.getClassLoader().getResourceAsStream(nomeArquivo))
        {
            if (input == null)
            {
                throw new RuntimeException("Arquivo " + nomeArquivo + " não encontrado.");
            }
            properties.load(input);
            System.out.println("Configuração carregada: " + nomeArquivo);
        } catch (IOException e)
        {
            throw new RuntimeException("Erro ao carregar configurações do arquivo " + nomeArquivo, e);
        }
    }

    public static String get(String key)
    {
        return properties.getProperty(key);
    }
}
