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
import java.util.Objects;
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
import br.syncdb.model.Coluna;
import br.syncdb.model.TableMetadata;


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
    public String criarEstuturaTabela(Connection conexao, String nomeTabela) throws SQLException
    {
        StringBuilder createTableScript = new StringBuilder();
        boolean needsUuidOssp = false;
        
        System.out.println("Criando a tabela: " + nomeTabela);

        createTableScript.append("CREATE TABLE ").append(nomeTabela).append(" (\n");
    
        DatabaseMetaData metaData = conexao.getMetaData();
        ResultSet resultadoQuery = metaData.getColumns(null, "public", nomeTabela, null);
    
        Set<String> primaryKeyColumns = new HashSet<>();
    
        while (resultadoQuery.next())
        {
            String nomeColuna = resultadoQuery.getString("COLUMN_NAME").trim();
            String tipoColuna = resultadoQuery.getString("TYPE_NAME").trim();
            String nullable = resultadoQuery.getString("NULLABLE");
            String defaultColuna = resultadoQuery.getString("COLUMN_DEF");
    
            createTableScript.append("    ")
                             .append(nomeColuna).append(" ")
                             .append(tipoColuna);
    
            if ("0".equals(nullable))
            {
                createTableScript.append(" NOT NULL");
            }
    
            if (defaultColuna != null && !defaultColuna.isEmpty())
            {
                if (!defaultColuna.toLowerCase().contains("uuid_generate_v4()") || !needsUuidOssp)
                {
                    if (defaultColuna.toLowerCase().contains("uuid_generate_v4()")) {
                        needsUuidOssp = true;
                    }
                    else if (defaultColuna != null && !defaultColuna.isEmpty()) 
                    {                    
                        createTableScript.append(" DEFAULT ").append(defaultColuna);
                    }
                }
            }
    
    
            createTableScript.append(",\n");
        }
    
        resultadoQuery.close();
    
        try (ResultSet pkResultSet = metaData.getPrimaryKeys(null, "public", nomeTabela))
        {
            while (pkResultSet.next())
            {
                String pkColumnName = pkResultSet.getString("COLUMN_NAME");
                primaryKeyColumns.add(pkColumnName);
            }
        }
    
        if (!primaryKeyColumns.isEmpty())
        {
            createTableScript.append("    PRIMARY KEY (")
                             .append(String.join(", ", primaryKeyColumns))
                             .append(")\n");
        }
        else
        {
            int lastCommaIndex = createTableScript.lastIndexOf(",");
            if (lastCommaIndex != -1)
            {
                createTableScript.deleteCharAt(lastCommaIndex);
            }
        }
    
        createTableScript.append(");\n");
    
        if (needsUuidOssp)
        {
            createTableScript.insert(0, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n");
        }
    
        return createTableScript.length() > 0 ?  createTableScript.toString() : null;
    
    }


    public  String  criarSequenciaQuery(Connection conexaoCloud, Connection conexaoLocal)  throws SQLException
    {
        StringBuilder createTableScript = new StringBuilder();

        String querySequencias = "SELECT schemaname, sequencename FROM pg_sequences WHERE schemaname = 'public';";  // Ajuste o esquema, se necessário
        try (Statement stmtCloud = conexaoCloud.createStatement();
             ResultSet rsCloud = stmtCloud.executeQuery(querySequencias)) {
    
            while (rsCloud.next())
            {
                String nomeSequenciaCloud = rsCloud.getString("sequencename");
    
               

                if (!sequenciaExiste(conexaoLocal, nomeSequenciaCloud))
                {
                    String createSequenceQuery = String.format(
                        "CREATE SEQUENCE IF NOT EXISTS %s START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;",
                        nomeSequenciaCloud);
                    createTableScript.append(createSequenceQuery+"\n");
                }
                

            }
        }

        return createTableScript.length() > 0 ?  createTableScript.toString() : null;
        
    }

    private boolean sequenciaExiste(Connection conexao, String nomeSequencia) throws SQLException {
        String query = "SELECT COUNT(*) FROM pg_class WHERE relname = ? AND relkind = 'S'"; // 'S' para sequência
        try (PreparedStatement stmt = conexao.prepareStatement(query)) {
            stmt.setString(1, nomeSequencia.trim().toLowerCase());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    public  String  criarFuncoesQuery(Connection conexao,  Connection conexaoLocal)  throws SQLException
    {

        StringBuilder createTableScript = new StringBuilder();

        String queryFuncoes = "SELECT n.nspname AS schema_name, p.proname AS function_name, pg_get_functiondef(p.oid) AS function_definition " +
                                "FROM pg_proc p " +
                                "JOIN pg_namespace n ON n.oid = p.pronamespace " +
                                "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND pg_function_is_visible(p.oid);";

        try (Statement stmtCloud = conexao.createStatement();
        ResultSet rsCloud = stmtCloud.executeQuery(queryFuncoes))
        {

            while (rsCloud.next())
            {
                String nomeFuncaoCloud = rsCloud.getString("function_name");
                String schemaCloud = rsCloud.getString("schema_name");
                String functionDefinitionCloud = rsCloud.getString("function_definition");

                
                if (!funcaoExiste(conexaoLocal, schemaCloud, nomeFuncaoCloud)) {
                    String createFunctionQuery = "CREATE FUNCTION " + schemaCloud + "." + nomeFuncaoCloud + " " + functionDefinitionCloud;
                    createTableScript.append(createFunctionQuery).append("\n");
                }
            }
        }

        return createTableScript.toString();
    }

    private boolean funcaoExiste(Connection conexao, String schema, String nomeFuncao) throws SQLException {
        String query = """
                SELECT COUNT(*) 
                FROM pg_proc p 
                JOIN pg_namespace n ON n.oid = p.pronamespace 
                WHERE n.nspname = ? AND p.proname = ?;
                """;
    
        try (PreparedStatement stmt = conexao.prepareStatement(query)) {
            stmt.setString(1, schema.trim().toLowerCase());
            stmt.setString(2, nomeFuncao.trim().toLowerCase());
    
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
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

    public  Set<String>  obterTabelaMetaData(String base, Connection conexao)
    {
        try
        {
            if( conexao == null)
            {
                return null;
            }

            DatabaseMetaData conexaoMetaData = conexao.getMetaData();
            ResultSet tabelas = conexaoMetaData.getTables(null, null, "%", new String[] {"TABLE"});
            
            Set<String> nomeTabelas = new HashSet<>();
            while (tabelas.next())
            {
                nomeTabelas.add(tabelas.getString("TABLE_NAME"));
            }

            return nomeTabelas;
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            ConexaoBanco.fecharConexao(base);
        }
    }

    public  Set<String>  obterColunaMetaData(String base, Connection conexao, String nomeTabela)
    {
        try
        {
            if( conexao == null)
            {
                return null;
            }

            DatabaseMetaData conexaoMetaData = conexao.getMetaData();
            ResultSet colunas = conexaoMetaData.getColumns(null, null, nomeTabela, null);
            
            Set<String> nomeColunas = new HashSet<>();
            
            while (colunas.next()) {
                nomeColunas.add(colunas.getString("COLUMN_NAME"));
            }

            return nomeColunas;
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            ConexaoBanco.fecharConexao(base);
        }
    }

    public TableMetadata obterTodosMetadados(Connection conexao, String nomeTabela) throws SQLException {
        String estrutura = criarEstuturaTabela(conexao, nomeTabela);
        String chaves = obterChaveEstrangeira(conexao, nomeTabela);
        return new TableMetadata(estrutura, chaves);
    }
    


    /////////////////////////
    public String compararEstruturaTabela(Connection conexaoCloud, Connection conexaoLocal, 
            String nomeTabela) throws SQLException {
        
        StringBuilder alteracoes = new StringBuilder();
        
        System.out.println("Verificando alteracao na tabela: " + nomeTabela);

        Map<String, Coluna> estruturaCloud = obterEstruturaColunas(conexaoCloud, nomeTabela);
        Map<String, Coluna> estruturaLocal = obterEstruturaColunas(conexaoLocal, nomeTabela);
        
        compararColunas(alteracoes, nomeTabela, estruturaCloud, estruturaLocal);
        compararColunasRemovidas(alteracoes, nomeTabela, estruturaCloud, estruturaLocal);
        
        return alteracoes.length() > 0 ?  alteracoes.toString() : null;
    }

    // Método auxiliar para obter estrutura de colunas
    private Map<String, Coluna> obterEstruturaColunas(Connection conexao, String nomeTabela) 
            throws SQLException {
        
        Map<String, Coluna> estrutura = new HashMap<>();
        DatabaseMetaData metaData = conexao.getMetaData();
        
        try (ResultSet colunas = metaData.getColumns(null, "public", nomeTabela, null)) {
            while (colunas.next()) {
                Coluna coluna = new Coluna();
                coluna.setNome(colunas.getString("COLUMN_NAME"));
                coluna.setTipo(colunas.getString("TYPE_NAME"));
                coluna.setNullable(colunas.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
                coluna.setDefaultValor(colunas.getString("COLUMN_DEF"));
                
                estrutura.put(coluna.getNome().toLowerCase(), coluna);
            }
        }
        
        return estrutura;
    }

    // Método auxiliar para comparar colunas existentes
    private void compararColunas(StringBuilder alteracoes, String nomeTabela,
            Map<String, Coluna> estruturaCloud, Map<String, Coluna> estruturaLocal) {
        
        estruturaCloud.forEach((nomeColuna, colunaCloud) -> {
            Coluna colunaLocal = estruturaLocal.get(nomeColuna);
            
            if (colunaLocal == null) {
                // Adicionar nova coluna
                alteracoes.append(String.format(
                    "ALTER TABLE %s ADD COLUMN %s %s%s%s;%n",
                    nomeTabela,
                    colunaCloud.getNome(),
                    colunaCloud.getTipo(),
                    colunaCloud.isNullable() ? "" : " NOT NULL",
                    colunaCloud.getDefaultValor() != null ? 
                        " DEFAULT " + colunaCloud.getDefaultValor() : ""
                ));
            } else {
                // Verificar diferenças
                compararTipoColuna(alteracoes, nomeTabela, nomeColuna, colunaCloud, colunaLocal);
                compararNullableColuna(alteracoes, nomeTabela, nomeColuna, colunaCloud, colunaLocal);
                compararDefaultColuna(alteracoes, nomeTabela, nomeColuna, colunaCloud, colunaLocal);
            }
        });
    }

    // Métodos específicos para cada tipo de comparação
    private void compararTipoColuna(StringBuilder alteracoes, String nomeTabela, String nomeColuna,
            Coluna cloud, Coluna local) {
        if (!cloud.getTipo().equalsIgnoreCase(local.getTipo())) {
            alteracoes.append(String.format(
                "ALTER TABLE %s ALTER COLUMN %s TYPE %s;%n",
                nomeTabela, nomeColuna, cloud.getTipo()
            ));
        }
    }

    private void compararNullableColuna(StringBuilder alteracoes, String nomeTabela, String nomeColuna,
            Coluna cloud, Coluna local) {
        if (cloud.isNullable() != local.isNullable()) {
            alteracoes.append(String.format(
                "ALTER TABLE %s ALTER COLUMN %s %s NOT NULL;%n",
                nomeTabela, nomeColuna, cloud.isNullable() ? "DROP" : "SET"
            ));
        }
    }

    private void compararDefaultColuna(StringBuilder alteracoes, String nomeTabela, String nomeColuna,
            Coluna cloud, Coluna local) {
        if (!Objects.equals(cloud.getDefaultValor(), local.getDefaultValor())) {
            if (cloud.getDefaultValor() == null) {
                alteracoes.append(String.format(
                    "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;%n",
                    nomeTabela, nomeColuna
                ));
            } else {
                alteracoes.append(String.format(
                    "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;%n",
                    nomeTabela, nomeColuna, cloud.getDefaultValor()
                ));
            }
        }
    }

    // Método auxiliar para colunas removidas
    private void compararColunasRemovidas(StringBuilder alteracoes, String nomeTabela,
            Map<String, Coluna> estruturaCloud, Map<String, Coluna> estruturaLocal) {
        
        estruturaLocal.keySet().stream()
            .filter(nomeColuna -> !estruturaCloud.containsKey(nomeColuna))
            .forEach(nomeColuna -> alteracoes.append(String.format(
                "ALTER TABLE %s DROP COLUMN %s;%n",
                nomeTabela, nomeColuna
            )));
    }

}
