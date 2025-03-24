package br.syncdb.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

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
        boolean needsUuidOssp = false;
        
        createTableScript.append("CREATE TABLE ").append(nomeTabela).append(" (\n");
    
        DatabaseMetaData metaData = conexao.getMetaData();
        ResultSet resultadoQuery = metaData.getColumns(null, "public", nomeTabela, null);
    
        Set<String> primaryKeyColumns = new HashSet<>();
    
        while (resultadoQuery.next()) {
            String nomeColuna = resultadoQuery.getString("COLUMN_NAME").trim();
            String tipoColuna = resultadoQuery.getString("TYPE_NAME").trim();
            String nullable = resultadoQuery.getString("NULLABLE");
            String defaultColuna = resultadoQuery.getString("COLUMN_DEF");
    
            createTableScript.append("    ")
                             .append(nomeColuna).append(" ")
                             .append(tipoColuna);
    
            if ("0".equals(nullable)) {
                createTableScript.append(" NOT NULL");
            }
    
            if (defaultColuna != null && !defaultColuna.isEmpty()) {
                if (!defaultColuna.toLowerCase().contains("uuid_generate_v4()") || !needsUuidOssp) {
                    if (defaultColuna.toLowerCase().contains("uuid_generate_v4()")) {
                        needsUuidOssp = true;
                    }
                    // createTableScript.append(" DEFAULT ").append(defaultColuna);
                }
            }
    
    
            createTableScript.append(",\n");
        }
    
        resultadoQuery.close();
    
        // Obter chaves primárias
        try (ResultSet pkResultSet = metaData.getPrimaryKeys(null, "public", nomeTabela)) {
            while (pkResultSet.next()) {
                String pkColumnName = pkResultSet.getString("COLUMN_NAME");
                primaryKeyColumns.add(pkColumnName);
            }
        }
    
        // Adiciona a chave primária ao script
        if (!primaryKeyColumns.isEmpty()) {
            createTableScript.append("    PRIMARY KEY (")
                             .append(String.join(", ", primaryKeyColumns))
                             .append(")\n");
        } else {
            // Remove a última vírgula se não houver chave primária
            int lastCommaIndex = createTableScript.lastIndexOf(",");
            if (lastCommaIndex != -1) {
                createTableScript.deleteCharAt(lastCommaIndex);
            }
        }
    
        createTableScript.append(");\n");
    
        if (needsUuidOssp) {
            createTableScript.insert(0, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n");
        }
    
        return createTableScript.toString();
    }

    public String obterChaveEstrangeira(Connection conexao, String nomeTabela) throws SQLException {
        StringBuilder createForeignKeyScript = new StringBuilder();
        
        DatabaseMetaData metaData = conexao.getMetaData();
        ResultSet foreignKeyResultSet = metaData.getImportedKeys(null, "public", nomeTabela);
    
        while (foreignKeyResultSet.next()) {
            String constraintName = foreignKeyResultSet.getString("FK_NAME");
            String columnName = foreignKeyResultSet.getString("FKCOLUMN_NAME");
            String foreignTableName = foreignKeyResultSet.getString("PKTABLE_NAME");
            String foreignColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME");
    
            if (constraintName != null && columnName != null && foreignTableName != null && foreignColumnName != null) {

                constraintName = constraintName.replace("-", "_");
                
                createForeignKeyScript.append("ALTER TABLE ").append(nomeTabela)
                                      .append(" ADD CONSTRAINT ").append(constraintName)
                                      .append(" FOREIGN KEY (").append(columnName).append(")")
                                      .append(" REFERENCES ").append(foreignTableName)
                                      .append(" (").append(foreignColumnName).append(")")
                                      .append(" ON DELETE CASCADE ON UPDATE CASCADE;\n");
            }
        }
    
        foreignKeyResultSet.close();
    
      
    
        return createForeignKeyScript.toString();
    }
    
    
   public String obterIndices(Connection conexao, String nomeTabela) throws SQLException {
    StringBuilder createIndexScript = new StringBuilder();
    DatabaseMetaData metaData = conexao.getMetaData();

    Map<String, List<String>> indices = new HashMap<>();

    ResultSet indexResultSet = metaData.getIndexInfo(null, "public", nomeTabela, false, false);

    while (indexResultSet.next()) {
        String indexName = indexResultSet.getString("INDEX_NAME");
        String columnName = indexResultSet.getString("COLUMN_NAME");
        boolean nonUnique = indexResultSet.getBoolean("NON_UNIQUE");

        if (indexName != null && columnName != null) {
            indices.computeIfAbsent(indexName, k -> new ArrayList<>()).add(columnName);
            indices.put(indexName + "_type", List.of(nonUnique ? "INDEX" : "UNIQUE INDEX"));
        }
    }

    for (String indexName : indices.keySet()) {
        if (!indexName.endsWith("_type")) {
            String indexType = indices.get(indexName + "_type").get(0);
            StringJoiner columns = new StringJoiner(", ");
            indices.get(indexName).forEach(columns::add);

            createIndexScript.append("CREATE ")
                             .append(indexType)
                             .append(" ").append(indexName)
                             .append(" ON ").append(nomeTabela)
                             .append(" (").append(columns).append(");\n");
        }
    }

    indexResultSet.close();

    if (createIndexScript.length() == 0) {
        return "-- Nenhum índice encontrado para a tabela " + nomeTabela + ".\n";
    }

    return createIndexScript.toString();
}

    
}
