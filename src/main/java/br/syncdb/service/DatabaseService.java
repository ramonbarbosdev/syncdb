package br.syncdb.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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

    public List<String> listarBases(String nomeBase, TipoConexao  tipo)
    {
        List<String> bases = new ArrayList<>();

        try (Connection conexao = ConexaoBanco.abrirConexao(nomeBase, tipo))
        {
            String query = "SELECT datname FROM pg_database WHERE datistemplate = false";

            var stmt = conexao.createStatement();
            var rs = stmt.executeQuery(query);

            while (rs.next()) {
                bases.add(rs.getString("datname"));
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        finally
        {
            ConexaoBanco.fecharConexao(nomeBase);
        }
        return bases;
    }

    public List<String> obterBanco(String base, String banco, TipoConexao  tipo) 
    {

        List<String> databases = listarBases( base, tipo);
        List<String> listarTabelas = new ArrayList<>();

        if(databases == null)
        {
            return null;
        }

        try
        {
            Connection conexao = ConexaoBanco.abrirConexao(base, tipo);

            for (String database : databases)
            {
                if(banco.trim().equalsIgnoreCase(database.trim()))
                {
                    String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
                    var stmt = conexao.createStatement();
                    var rs = stmt.executeQuery(query);
                
                    while (rs.next())
                    {
                        String tableName = rs.getString("table_name");
                        listarTabelas.add(tableName);
                    }
                }
            }

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        finally
        {
            ConexaoBanco.fecharConexao(base);
        }
            
        return listarTabelas;
    }

   


    public boolean verificarTabelaExistente( String base, TipoConexao tipo, String tabelaNome) throws SQLException
    {

        try
        {
            Connection conexao = ConexaoBanco.abrirConexao(base, tipo);

            String query = "SELECT EXISTS (" +
                        "SELECT 1 " +
                        "FROM information_schema.tables " +
                        "WHERE table_schema = 'public' " +
                        "AND table_name = '" + tabelaNome + "'" +
                        ");";

                var stmt = conexao.createStatement();
                var rs = stmt.executeQuery(query);

                if (rs.next())
                {
                return rs.getBoolean(1); 
                }

        } catch (Exception e) {
            // TODO: handle exception
        }

       
        return false; 
    }
    public String obterEstruturaTabela(Connection conexao, String nomeTabela) throws SQLException {
        StringBuilder createTableScript = new StringBuilder();
        boolean needsUuidOssp = false; // Flag para verificar se a extensão é necessária
    
        createTableScript.append("CREATE TABLE " + nomeTabela + " (\n");
    
        // Obter DatabaseMetaData
        DatabaseMetaData metaData = conexao.getMetaData();
        ResultSet resultadoQuery = metaData.getColumns(null, "public", nomeTabela, null);
    
        while (resultadoQuery.next()) {
            String nomeColuna = resultadoQuery.getString("COLUMN_NAME").trim();
            String tipoColuna = resultadoQuery.getString("TYPE_NAME").trim();
            String nullable = resultadoQuery.getString("NULLABLE").trim();
            String defaultColuna = resultadoQuery.getString("COLUMN_DEF");
    
            createTableScript.append("    ")
                    .append(nomeColuna).append(" ")
                    .append(tipoColuna);
    
            if ("0".equals(nullable)) { // 0 significa NOT NULL
                createTableScript.append(" NOT NULL");
            }
    
            // Verifica se a coluna usa uuid_generate_v4() como valor padrão
            if (defaultColuna != null && !defaultColuna.isEmpty() && defaultColuna.toLowerCase().contains("uuid_generate_v4()")) {
                needsUuidOssp = true; 
            }
    
            // if (defaultColuna != null && !defaultColuna.isEmpty() && !tipoColuna.equalsIgnoreCase("serial")) {
            //     createTableScript.append(" DEFAULT ").append(defaultColuna);
            // }
    
            createTableScript.append(",\n");
        }
    
        if (createTableScript.length() > 2) {
            createTableScript.setLength(createTableScript.length() - 2); // Remove a última vírgula
        }
    
        createTableScript.append("\n);\n");
    
        // Se a extensão for necessária, adiciona a criação da extensão no início do script
        if (needsUuidOssp) {
            createTableScript.insert(0, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n");
        }
    
        return createTableScript.toString();
    }
    
    // public static String criarSequenciaQuery(Connection conexao, String tabelaOrigem) {
    //     try {
    //         DatabaseMetaData metaData = conexao.getMetaData();
    //         ResultSet resultadoQuery = metaData.getColumns(null, "public", tabelaOrigem, null);
    
    //         // Criar sequência
    //         StringBuilder createSequenceQuery = new StringBuilder();
    
    //         while (resultadoQuery.next()) {
    //             String nomeColunaSeq = resultadoQuery.getString("COLUMN_NAME");
    //             String columnDefault = resultadoQuery.getString("COLUMN_DEF");
    //             String tipoColuna = resultadoQuery.getString("TYPE_NAME");
    
    //             // Verifica se a coluna tem uma sequência associada e não é do tipo serial
    //             if (columnDefault != null && columnDefault.toLowerCase().contains("nextval") && !tipoColuna.equalsIgnoreCase("serial")) {
    //                 createSequenceQuery.append("CREATE SEQUENCE IF NOT EXISTS ")
    //                         .append(tabelaOrigem).append("_").append(nomeColunaSeq).append("_seq ")
    //                         .append("START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1; ");
    //             }
    //         }
    
    //         return createSequenceQuery.toString();
    
    //     } catch (SQLException e) {
    //         e.printStackTrace();
    //     }
    //     return "";
    // }
    public String obterChaveEstrangeira(Connection conexao, String nomeTabela) throws SQLException
    {
        StringBuilder createTableScript = new StringBuilder();

        String foreignKeyQuery = "SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name " +
                                      "FROM information_schema.table_constraints AS tc " +
                                      "JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name " +
                                      "JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name " +
                                      "WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name = '" + nomeTabela + "';";



            var stmt = conexao.createStatement();
            var foreignKeyResultSet = stmt.executeQuery(foreignKeyQuery);

            while (foreignKeyResultSet.next())
            {
                String constraintName = foreignKeyResultSet.getString("constraint_name");
                String columnName = foreignKeyResultSet.getString("column_name");
                String foreignTableName = foreignKeyResultSet.getString("foreign_table_name");
                String foreignColumnName = foreignKeyResultSet.getString("foreign_column_name");

                createTableScript.append("ALTER TABLE ").append(nomeTabela)
                                 .append(" ADD CONSTRAINT ").append(constraintName)
                                 .append(" FOREIGN KEY (").append(columnName)
                                 .append(") REFERENCES ").append(foreignTableName)
                                 .append("(").append(foreignColumnName).append(");\n");
            }

       
        return createTableScript.toString();
    }
    public String obterIndices(Connection conexao, String nomeTabela) throws SQLException
    {
        StringBuilder createTableScript = new StringBuilder();

        String indexQuery = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '" + nomeTabela + "';";

        var stmt = conexao.createStatement();
        var indexResultSet = stmt.executeQuery(indexQuery);

        while (indexResultSet.next())
        {
            String indexName = indexResultSet.getString("indexname");
            String indexDef = indexResultSet.getString("indexdef");

            createTableScript.append(indexDef).append(";\n");
        }
       
        return createTableScript.toString();
    }

    
}
