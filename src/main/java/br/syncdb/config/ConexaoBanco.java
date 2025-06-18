package br.syncdb.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import br.syncdb.controller.TipoConexao;
import br.syncdb.utils.SqliteUtils;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class ConexaoBanco {

    private static final Dotenv dotenv;




    static {
        try {
            dotenv = Dotenv.configure()
                            // .directory("src/main/resources/.env")  
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
       

        Map<String, String> response = gerenciarConexao(  database,  tipo , false);

        String host = response.get("host");
        String port = response.get("port");
        String user = response.get("user");
        String password = response.get("password");


        String chave = database + "_" + tipo.name(); 
    
        HikariDataSource dataSource = dataSourceMap.get(chave);

        if (host == null || host.isBlank() || port == null || port.isBlank()) {
            throw new SQLException("Dados incompletos: host ou porta ausentes ao tentar conectar com o banco " + tipo);
        }

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
  

    public static Map<String, String> gerenciarConexao(String database, TipoConexao tipo, Boolean form) throws SQLException
    {
        Map<String, String> response = new HashMap<String, String>();

        String host = "";
        String port = "";
        String user = "";
        String password = "";

        Map<String, String> dados = buscarDadosConexao(tipo);

        if(  dados != null && dados.size() > 0 )
        {
            if (dados == null) throw new SQLException("Falha ao buscar dados da conexão para: " + tipo);
            
            host = dados.get("host");
            port = dados.get("port");
            user = dados.get("user");
            password = dados.get("password");
           
        }
        else
        {

            if(tipo.equals(TipoConexao.LOCAL))
            {
                host = ConfigPropertiesBanco.get("spring.datasource.host");
                port = ConfigPropertiesBanco.get("spring.datasource.port");
                user = ConfigPropertiesBanco.get("spring.datasource.username");
                password = ConfigPropertiesBanco.get("spring.datasource.password");
            }
            else if(tipo.equals(TipoConexao.CLOUD))
            {
                host = dotenv.get("DATABASE_" +tipo+ "_HOST");
                port = dotenv.get("DATABASE_" +tipo+ "_PORT");
                user = dotenv.get("DATABASE_" +tipo+ "_USER");
                password  = dotenv.get("DATABASE_" +tipo+ "_PASS");
            }
            else
            {
                throw new SQLException("Tipo de conexão inválido: " + tipo);
            }


        }

        validacaoConecao(database, tipo, dados);

        response.put("host", host );
        response.put("port", port );
        response.put("user", user );
        response.put("password", password );

        return response;
    }

    public static void validacaoConecao(String database, TipoConexao tipo,Map<String, String> dados  ) throws SQLException
    {
        if (database == null || database.isEmpty()) throw new IllegalArgumentException("O nome do banco de dados não pode ser nulo ou vazio.");

        if (tipo == null) throw new IllegalArgumentException("O tipo de conexão não pode ser nulo.");

        if( dados != null && dados.size() > 0)
        {
            if( dados.get("host") == null ) throw new IllegalArgumentException("O host do banco de dados "+tipo+" não pode ser nulo ou vazio. Verifique as Conexões!");
            if( dados.get("port") == null ) throw new IllegalArgumentException("A porta do banco de dados "+tipo+" não pode ser nulo ou vazio. Verifique as Conexões!");
            if( dados.get("user") == null ) throw new IllegalArgumentException("O usuário do banco de dados "+tipo+" não pode ser nulo ou vazio. Verifique as Conexões!");
            if( dados.get("password") == null ) throw new IllegalArgumentException("A senha do banco de dados "+tipo+" não pode ser nulo ou vazio. Verifique as Conexões!");
        }

        if (dotenv.get("DATABASE_" + TipoConexao.CLOUD + "_HOST") == null ) {
            throw new IllegalArgumentException("O host do banco de dados "+tipo+" não pode ser nulo ou vazio. [ARQUIVO .ENV]" );
        }
        if (dotenv.get("DATABASE_" + TipoConexao.CLOUD + "_PORT") == null ) {
            throw new IllegalArgumentException("A porta do banco de dados "+tipo+" não pode ser nula ou vazia. [ARQUIVO .ENV]");
        }
        if (dotenv.get("DATABASE_" + TipoConexao.CLOUD + "_USER") == null ) {
            throw new IllegalArgumentException("O usuário do banco de dados "+tipo+"não pode ser nulo ou vazio. [ARQUIVO .ENV]");
        }
        if (dotenv.get("DATABASE_" + TipoConexao.CLOUD + "_PASS") == null) {
            throw new IllegalArgumentException("A senha do banco de dados "+tipo+ "não pode ser nula ou vazia. [ARQUIVO .ENV]");
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

    
     public static Map<String, String> buscarDadosConexao(TipoConexao tipo) {
        String urlBaseDbPath = ConfigPropertiesBanco.get("app.db.path"); 

        String jdbcUrl = SqliteUtils.montaJdbcUrl(urlBaseDbPath);
      
        String arquivoPath = SqliteUtils.extrairCaminhoArquivo(jdbcUrl);
        SqliteUtils.garantirDiretorioPai(arquivoPath);
    
        try (Connection conexao = DriverManager.getConnection(jdbcUrl)) {
            StringBuilder query = new StringBuilder();
            if (tipo == TipoConexao.CLOUD) {
                query.append("select db_cloud_host as host, db_cloud_password as password, db_cloud_port as port, db_cloud_user as user from conexao limit 1");
            } else if (tipo == TipoConexao.LOCAL) {
                query.append("select db_local_host as host, db_local_password as password, db_local_port as port, db_local_user as user from conexao limit 1");
            } else {
                throw new SQLException("Tipo de conexão inválido: " + tipo);
            }

            try (PreparedStatement stmt = conexao.prepareStatement(query.toString());
                 ResultSet rs = stmt.executeQuery()) {

                if (rs.next()) {
                    Map<String, String> dados = new HashMap<>();
                    adicionarValido(dados, "host", rs.getString("host"));
                    adicionarValido(dados, "port", rs.getString("port"));
                    adicionarValido(dados, "user", rs.getString("user"));
                    adicionarValido(dados, "password", rs.getString("password"));
                    return dados;
                } else {
                    System.out.println("[ConexaoBanco] Nenhum registro encontrado na tabela 'conexao' para tipo " + tipo);
                }
            }
        } catch (SQLException e) {
            System.err.println("[ConexaoBanco] Erro ao buscar dados de conexão: " + e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    private static void adicionarValido(Map<String, String> mapa, String chave, String valor)
    {
        if (valor != null && !valor.trim().isEmpty()) {
            mapa.put(chave, valor);
        }
    }
    
}
