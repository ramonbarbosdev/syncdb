package br.syncdb.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import br.syncdb.controller.TipoConexao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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
        
        Map<String, String> dados = buscarDadosConexao(tipo);

        if (dados == null) throw new SQLException("Falha ao buscar dados da conexão para: " + tipo);
        
        String host = dados.get("host");
        String port = dados.get("port");
        String user = dados.get("user");
        String password = dados.get("password");

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

    public static Map<String, String>  buscarDadosConexao( TipoConexao tipo) 
    {
    
        String user = ConfiguracaoBanco.get("spring.datasource.username");
        String password = ConfiguracaoBanco.get("spring.datasource.password");
        String url = ConfiguracaoBanco.get("spring.datasource.url");
        
        try( Connection conexao = DriverManager.getConnection(  url, user,password);)
        {   
            StringBuilder query = new StringBuilder();

            if(tipo == TipoConexao.CLOUD)
            {
                query.append(
                    "select  db_cloud_host as host, db_cloud_password as password, db_cloud_port as port, db_cloud_user  as user from conexao limit 1 "
                );
            }
            else if(tipo == TipoConexao.LOCAL)
            {
                query.append(
                    "select db_local_host as host , db_local_password as password, db_local_port as port, db_local_user as user from conexao limit 1"
                );
            }
            else
            {
                throw new SQLException("Tipo de conexão inválido: " + tipo);
            }

            PreparedStatement stmt = conexao.prepareStatement(query.toString());

            ResultSet rs = stmt.executeQuery();

           if (rs.next())
           {
                Map<String, String> dados = new HashMap<>();
                dados.put("host", rs.getString("host"));
                dados.put("port", rs.getString("port"));
                dados.put("user", rs.getString("user"));
                dados.put("password", rs.getString("password"));
                return dados;
            } 
          
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return null;
       
    }
    
}
