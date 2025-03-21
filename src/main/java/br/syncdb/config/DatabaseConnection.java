package br.syncdb.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConnection
{
    private String url;
    private String user;
    private String password;

    public DatabaseConnection(String tipoConexao)
    {
        carregarConfiguracoes(tipoConexao);
    }

    private void carregarConfiguracoes(String tipoConexao)
    {

        
        Properties properties = new Properties();
        String configFileName = "application.properties"; 

        try (FileInputStream input = new FileInputStream(configFileName))
        {
            properties.load(input);
          
            if ("local".equalsIgnoreCase(tipoConexao))
            {
                this.url = properties.getProperty("db.local.url");
                this.user = properties.getProperty("db.local.user");
                this.password = properties.getProperty("db.local.password");
            }
            else if ("cloud".equalsIgnoreCase(tipoConexao))
            {
                this.url = properties.getProperty("db.cloud.url");
                this.user = properties.getProperty("db.cloud.user");
                this.password = properties.getProperty("db.cloud.password");
            }
            else
            {
                throw new IllegalArgumentException("Tipo de conexão inválido: " + tipoConexao);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    public Connection abrirConexao()
    {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

}