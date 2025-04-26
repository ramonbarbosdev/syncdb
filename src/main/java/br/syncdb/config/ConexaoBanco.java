package br.syncdb.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import br.syncdb.controller.TipoConexao;
import io.github.cdimascio.dotenv.Dotenv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConexaoBanco {

    private static final Dotenv dotenv;

    static {
        try {
            dotenv = Dotenv.configure()
                            .directory("./.env")  
                            .load();
                            
        } catch (Exception e) {
            throw new ExceptionInInitializerError("Erro ao carregar o arquivo .env: " + e.getMessage());
        }
    }


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

   
    public static Connection abrirConexao(String database, TipoConexao tipo) throws SQLException
    {
        validacaoConecao(database, tipo);

        String host = dotenv.get("DATABASE_" +tipo+ "_HOST");
        String port = dotenv.get("DATABASE_" +tipo+ "_PORT");
        String user = dotenv.get("DATABASE_" +tipo+ "_USER");
        String password  = dotenv.get("DATABASE_" +tipo+ "_PASS");

        String chave = database + "_" + tipo.name(); 
    
        HikariDataSource dataSource = dataSourceMap.get(chave);
        if (dataSource == null)
        {
            synchronized (ConexaoBanco.class) {
                dataSource = dataSourceMap.get(chave);
                if (dataSource == null) {
                    dataSource = criarDataSource(host, port, database, user, password);
                    dataSourceMap.put(chave, dataSource);
                }
            }
        }
        
        return dataSource.getConnection();
    }

    public static void validacaoConecao(String database, TipoConexao tipo) throws SQLException
    {
        if (database == null || database.isEmpty())
        {
            throw new IllegalArgumentException("O nome do banco de dados não pode ser nulo ou vazio.");
        }
        if (tipo == null)
        {
            throw new IllegalArgumentException("O tipo de conexão não pode ser nulo.");
        }

        if (dotenv.get("DATABASE_" + tipo + "_HOST") == null || dotenv.get("DATABASE_" + tipo + "_HOST").isEmpty()) {
            throw new IllegalArgumentException("O host do banco de dados não pode ser nulo ou vazio.");
        }
        if (dotenv.get("DATABASE_" + tipo + "_PORT") == null || dotenv.get("DATABASE_" + tipo + "_PORT").isEmpty()) {
            throw new IllegalArgumentException("A porta do banco de dados não pode ser nula ou vazia.");
        }
        if (dotenv.get("DATABASE_" + tipo + "_USER") == null || dotenv.get("DATABASE_" + tipo + "_USER").isEmpty()) {
            throw new IllegalArgumentException("O usuário do banco de dados não pode ser nulo ou vazio.");
        }
        if (dotenv.get("DATABASE_" + tipo + "_PASS") == null || dotenv.get("DATABASE_" + tipo + "_PASS").isEmpty()) {
            throw new IllegalArgumentException("A senha do banco de dados não pode ser nula ou vazia.");
        }
        String chave = database + "_" + tipo.name(); 
    
        HikariDataSource dataSource = dataSourceMap.get(chave);
        if (dataSource != null && !dataSource.isClosed())
        {
            System.out.println("Pool de conexão já existe para o banco: " + chave);
        }
        else
        {
            System.out.println("Nenhum pool encontrado para o banco: " + chave);
        }
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
                dataSourceMap.remove(chave); 
            }
        } else {
            System.out.println("Nenhum pool encontrado para o banco: " + chave);
        }
    }

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
    
        String user = ConfigPropertiesBanco.get("spring.datasource.username");
        String password = ConfigPropertiesBanco.get("spring.datasource.password");
        String url = ConfigPropertiesBanco.get("spring.datasource.url");

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
