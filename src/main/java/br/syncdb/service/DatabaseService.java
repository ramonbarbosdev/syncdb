package br.syncdb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

import br.syncdb.config.ConexaoBanco;
import br.syncdb.config.DatabaseConnection;
import br.syncdb.controller.TipoConexao;


@Service
public class DatabaseService  
{
    
    // @Autowired
    // @Qualifier("cloudDataSource")
    // private JdbcTemplate jdbcTemplate;

    public List<String> listarBases(String nomeBase, TipoConexao  tipo) {
        List<String> bases = new ArrayList<>();
        try (Connection conexao = ConexaoBanco.abrirConexao(nomeBase, tipo)) {
            String query = "SELECT datname FROM pg_database WHERE datistemplate = false";
            try (var stmt = conexao.createStatement(); var rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    bases.add(rs.getString("datname"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return bases;
    }

    public List<String> obterBanco(String base, String banco, TipoConexao  tipo) 
    {
        // List<String> databases = listarBases(tipo, base);
        List<String> databases = listarBases( base, tipo);
        List<String> listarTabelas = new ArrayList<>();

        if(databases == null)
        {
            return null;
        }
        
        Statement conexao = abrirConexao(banco);
        
        for (String database : databases)
        {
            if(banco.trim().equalsIgnoreCase(database.trim()))
            {
                try
                {
                    String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
                    ResultSet resultSet = conexao.executeQuery(query);
    
                    while (resultSet.next())
                    {
                        String tableName = resultSet.getString("table_name");
                        listarTabelas.add(tableName);
                    }
                    
                }
                catch (Exception e)
                {
                    System.out.println(e.getMessage());
                }
            }
        }

        return listarTabelas;
    }


    public boolean verificarTabelaExistente(Statement conexao, String tabelaNome) throws SQLException {
        String query = "SELECT EXISTS (" +
                       "SELECT 1 " +
                       "FROM information_schema.tables " +
                       "WHERE table_schema = 'public' " +
                       "AND table_name = '" + tabelaNome + "'" +
                       ");";
    
        ResultSet resultadoQuery = conexao.executeQuery(query);
        if (resultadoQuery.next()) {
            return resultadoQuery.getBoolean(1); // Retorna true se a tabela existir
        }
        return false; // Retorna false se não houver resultado
    }

    public static String criarCriacaoTabelaQuery( Statement conexao ,String tabelaOrigem)
    {
        StringBuilder createTableScript = new StringBuilder();

        String scriptSequencia = criarSequenciaQuery( conexao,  tabelaOrigem);

        createTableScript.append(scriptSequencia+ "\n");

        createTableScript.append("CREATE TABLE " + tabelaOrigem + " (\n");

        try 
        {

            String query = "SELECT column_name, data_type, is_nullable, column_default " +
                           "FROM information_schema.columns " +
                           "WHERE table_name = '" + tabelaOrigem + "' " +
                           "AND table_schema = 'public';";

            ResultSet resultadoQuery = conexao.executeQuery(query);

            while (resultadoQuery.next()) {
                String nomeColuna = resultadoQuery.getString("column_name").trim();
                String tipoColuna = resultadoQuery.getString("data_type").trim();
                String nullable = resultadoQuery.getString("is_nullable").trim();
                String defaultColuna = resultadoQuery.getString("column_default");
            
                createTableScript.append("    ")
                        .append(nomeColuna).append(" ")
                        .append(tipoColuna);
            
                if ("NO".equalsIgnoreCase(nullable)) {
                    createTableScript.append(" NOT NULL");
                }
            
                if (defaultColuna != null && !defaultColuna.isEmpty()) {
                   
                    createTableScript.append(" DEFAULT ").append(defaultColuna);
                }
            
                createTableScript.append(",\n");
  
            }

            if (createTableScript.length() > 2) {
                createTableScript.setLength(createTableScript.length() - 2); 
            }

            createTableScript.append("\n);\n");

       

            //  chaves estrangeiras
            String foreignKeyQuery = "SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name " +
                                      "FROM information_schema.table_constraints AS tc " +
                                      "JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name " +
                                      "JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name " +
                                      "WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name = '" + tabelaOrigem + "';";

            ResultSet foreignKeyResultSet = conexao.executeQuery(foreignKeyQuery);
            while (foreignKeyResultSet.next()) {
                String constraintName = foreignKeyResultSet.getString("constraint_name");
                String columnName = foreignKeyResultSet.getString("column_name");
                String foreignTableName = foreignKeyResultSet.getString("foreign_table_name");
                String foreignColumnName = foreignKeyResultSet.getString("foreign_column_name");

                createTableScript.append("ALTER TABLE ").append(tabelaOrigem)
                                 .append(" ADD CONSTRAINT ").append(constraintName)
                                 .append(" FOREIGN KEY (").append(columnName)
                                 .append(") REFERENCES ").append(foreignTableName)
                                 .append("(").append(foreignColumnName).append(");\n");
            }

            // Obter índices
            String indexQuery = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '" + tabelaOrigem + "';";
            ResultSet indexResultSet = conexao.executeQuery(indexQuery);
            while (indexResultSet.next()) {
                String indexName = indexResultSet.getString("indexname");
                String indexDef = indexResultSet.getString("indexdef");

                createTableScript.append(indexDef).append(";\n");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return createTableScript.toString();
    }
    public static String criarSequenciaQuery(Statement conexao, String tabelaOrigem) 
    {
        try 
        {
            String query = "  select column_name  FROM information_schema.columns  " 
            + "WHERE table_name = '" + tabelaOrigem + "' " 
            + " and column_default ilike '%nextval%' ";

            ResultSet resultadoQuery = conexao.executeQuery(query);

            //criar sequencia 
            StringBuilder createSequenceQuery = new StringBuilder();

            while (resultadoQuery.next())
            {
                String nomeColunaSeq = resultadoQuery.getString("column_name");
                createSequenceQuery.append("CREATE SEQUENCE IF NOT EXISTS ")
                                .append(tabelaOrigem).append("_").append(nomeColunaSeq).append("_seq ")
                                .append("START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1; ");
            }

            return createSequenceQuery.toString();

        }
        catch (SQLException e)
        {
              e.printStackTrace();
        }
        return tabelaOrigem;
        
    }



    public Statement abrirConexao(String database) 
    {

        try
        {
            String host = "db-postgresql-nyc3-32073-do-user-9424476-0.b.db.ondigitalocean.com";
            String port = "25060";
            String user = "doadmin";
            String password = "AVNS_ThcFV7CzqE1EzYP7W8z";
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + database; 
    
            Connection connection = DriverManager.getConnection( url, user, password);
            Statement conexao = (Statement) connection.createStatement(); 
            return conexao;

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        
        return null;

    }

    public List<String> obterBanco(String base, String banco, String string) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'obterBanco'");
    }
   

}
