package br.syncdb.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import br.syncdb.controller.TipoConexao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConexaoBanco {


    private static final Map<String, HikariDataSource> dataSourceMap = new ConcurrentHashMap<>();

    private static HikariDataSource criarDataSource(String host, String port, String database, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + database);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setIdleTimeout(30000);
        config.setMaxLifetime(1800000);
        config.setConnectionTimeout(30000);
        return new HikariDataSource(config);
    }

   
    public static Connection abrirConexao(String database, TipoConexao tipo) throws SQLException {
        String prefixo = tipo.getPrefixo(); // exemplo: "local." ou "cloud."
        String host = ConfiguracaoBanco.get(prefixo + ".host");
        String port = ConfiguracaoBanco.get(prefixo + ".port");
        String user = ConfiguracaoBanco.get(prefixo + ".user");
        String password = ConfiguracaoBanco.get(prefixo + ".password");
    
        String chave = database + "_" + tipo.name(); 
    
        HikariDataSource dataSource = dataSourceMap.get(chave);
        if (dataSource == null)
        {
            System.err.println("Nova Conexao: "+database);
            synchronized (ConexaoBanco.class) {
                dataSource = dataSourceMap.get(chave);
                if (dataSource == null) {
                    dataSource = criarDataSource(host, port, database, user, password);
                    dataSourceMap.put(chave, dataSource);
                }
            }
        }
        else
        {
            System.err.println("Conexao existente: "+database);
        }
        
        return dataSource.getConnection();
    }
    
    

    public static void fecharConexao(String database, TipoConexao tipo) {
        String chave = database + "_" + tipo.name();
        HikariDataSource dataSource = dataSourceMap.get(chave);
    
        if (dataSource != null) {
            if (!dataSource.isClosed())
            {
                try
                {
                    dataSource.close();
                    System.out.println("Pool de conexão fechado para o banco: " + chave);
                }
                catch (Exception e)
                {
                    System.err.println("Erro ao fechar o pool para o banco " + chave + ": " + e.getMessage());
                }
                finally
                {
                    dataSourceMap.remove(chave);
                }
            }
            else
            {
                System.out.println("Pool já estava fechado para o banco: " + chave);
                dataSourceMap.remove(chave); // limpa do mapa mesmo assim
            }
        } else {
            System.out.println("Nenhum pool encontrado para o banco: " + chave);
        }
    }
    
    

    // Método para fechar todos os pools de conexão
    public static void fecharTodos()
    {
        for (Map.Entry<String, HikariDataSource> entry : dataSourceMap.entrySet())
        {
            String chave = entry.getKey();
            HikariDataSource dataSource = entry.getValue();
            if (dataSource != null)
            {
                dataSource.close();
                System.out.println("Pool de conexão fechado para: " + chave);
            }
        }
        dataSourceMap.clear(); 
    }
    
}
