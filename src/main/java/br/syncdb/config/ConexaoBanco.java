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

    

    // Mapa para armazenar os pools de conexão por banco
    private static final Map<String, HikariDataSource> dataSourceMap = new ConcurrentHashMap<>();

    // Método para criar dinamicamente um pool para uma base específica
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

   
    public static Connection abrirConexao(String database,  TipoConexao tipo) throws SQLException
    {
        String prefixo = tipo.getPrefixo();

        String host = ConfiguracaoBanco.get(prefixo + ".host");
        String port = ConfiguracaoBanco.get(prefixo + ".port");
        String user = ConfiguracaoBanco.get(prefixo + ".user");
        String password = ConfiguracaoBanco.get(prefixo + ".password");

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
        Connection connection = null;
        try
        {
            connection = DriverManager.getConnection(url, user, password);
        }
        catch (SQLException e)
        {
            // System.err.println(e.getMessage() + " Base: " + database + ". Conexao: "+ tipo); 
            throw new SQLException(e.getMessage() + " Base: " + database + ". Conexao: "+ tipo);
        }

        return connection; 

    }

    // Método para fechar o pool de conexão de uma base específica
    public static void fecharConexao(String database) {
        HikariDataSource dataSource = dataSourceMap.get(database);
        if (dataSource != null) {
            dataSource.close();
            dataSourceMap.remove(database);
            System.out.println("✅ Pool de conexão fechado para o banco: " + database);
        }
    }

    // Método para fechar todos os pools de conexão
    public static void fecharTodos() {
        for (String database : dataSourceMap.keySet()) {
            fecharConexao(database);
        }
    }
}
