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

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
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
import br.syncdb.utils.DicionarioTipoSql;


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

   

    public boolean schemaExiste(Connection conexao, String nomeSchema) throws SQLException
    {
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
        try (PreparedStatement stmt = conexao.prepareStatement(sql))
        {
            stmt.setString(1, nomeSchema);
            try (ResultSet rs = stmt.executeQuery())
            {
                return rs.next(); 
            }
        }
    }

    public String gerarQueryCriacaoSchemas(Connection conexao, String schema) throws SQLException
    {
        String query = "";

        if (!schemaExiste(conexao, schema)) query = "CREATE SCHEMA IF NOT EXISTS " + schema + ";";
    
        return query;
    }   
   
    public String criarEstuturaTabela(Connection conexao, String nomeTabelaCompleto) throws SQLException
    {
        StringBuilder createTableScript = new StringBuilder();
        boolean needsUuidOssp = false;
    
        String schema = "public";
        String nomeTabela = nomeTabelaCompleto;
    
        if (nomeTabelaCompleto.contains("."))
        {
            String[] partes = nomeTabelaCompleto.split("\\.");
            schema = partes[0];
            nomeTabela = partes[1];
        }
    
        Map<String, String> serialColumns = new HashMap<>();
        try (PreparedStatement stmt = conexao.prepareStatement(
                "SELECT column_name, column_default FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? AND column_default LIKE 'nextval%'")) {
            stmt.setString(1, schema);
            stmt.setString(2, nomeTabela);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String colName = rs.getString("column_name");
                String rawDefault = rs.getString("column_default"); 
                String clean = rawDefault.replace("::regclass", "").replace("nextval(", "").replace("'", "").replace(")", "").trim();
                String seqSemSchema = clean.contains(".") ? clean.split("\\.")[1] : clean;
                String seqDef = "nextval('" + seqSemSchema + "'::regclass)";
                serialColumns.put(colName, seqDef);
            }
        }
    
        // Construir a definição da tabela
        createTableScript.append("CREATE TABLE ").append(schema).append(".").append(nomeTabela).append(" (\n");
    
        DatabaseMetaData metaData = conexao.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, schema, nomeTabela, null)) {
            while (columns.next()) {
                String colName = columns.getString("COLUMN_NAME").trim();
                String typeName = columns.getString("TYPE_NAME").trim();
                boolean isNullable = "1".equals(columns.getString("NULLABLE"));
                String defaultValue = columns.getString("COLUMN_DEF");
    
                createTableScript.append("    ").append(colName).append(" ");
    
                if (serialColumns.containsKey(colName)) {
                    createTableScript.append("integer");
                } else {
                    createTableScript.append(typeName);
                }
    
                if (!isNullable) {
                    createTableScript.append(" NOT NULL");
                }
    
                if (serialColumns.containsKey(colName)) {
                    createTableScript.append(" DEFAULT ").append(serialColumns.get(colName));
                } else if (defaultValue != null && !defaultValue.isEmpty()) {
                    if (defaultValue.toLowerCase().contains("uuid_generate_v4()")) {
                        needsUuidOssp = true;
                        createTableScript.append(" DEFAULT uuid_generate_v4()");
                    } else {
                        String cleanDefault = defaultValue.split("::")[0].trim();
                        createTableScript.append(" DEFAULT ").append(cleanDefault);
                    }
                }
    
                createTableScript.append(",\n");
            }
        }
    
        // Adicionar chave primária
        try (ResultSet pkRs = metaData.getPrimaryKeys(null, schema, nomeTabela)) {
            List<String> pkColumns = new ArrayList<>();
            while (pkRs.next()) {
                pkColumns.add(pkRs.getString("COLUMN_NAME"));
            }
    
            if (!pkColumns.isEmpty()) {
                createTableScript.append("    PRIMARY KEY (")
                        .append(String.join(", ", pkColumns))
                        .append(")\n");
            } else {
                createTableScript.setLength(createTableScript.length() - 2);
                createTableScript.append("\n");
            }
        }
    
        createTableScript.append(");");
    
        if (needsUuidOssp) {
            createTableScript.insert(0, "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n\n");
        }
    
        return createTableScript.toString();
    }
    

    public  String  criarSequenciaQuery(Connection conexaoCloud, Connection conexaoLocal)  throws SQLException
    {
        StringBuilder createTableScript = new StringBuilder();

        String querySequencias = "SELECT schemaname, sequencename FROM pg_sequences "; 
        try (Statement stmtCloud = conexaoCloud.createStatement();
             ResultSet rsCloud = stmtCloud.executeQuery(querySequencias))
        {
    
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

   

    private boolean sequenciaExiste(Connection conexao, String nomeSequencia) throws SQLException
    {
        String query = "SELECT COUNT(*) FROM pg_class WHERE relname = ? AND relkind = 'S'"; // 'S' para sequência
        try (PreparedStatement stmt = conexao.prepareStatement(query))
        {
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


    public String obterChaveEstrangeira(Connection conexao, String nomeTabela) throws SQLException
    {
        StringBuilder createForeignKeyScript = new StringBuilder();
        
        DatabaseMetaData metaData = conexao.getMetaData();
        ResultSet foreignKeyResultSet = metaData.getImportedKeys(null, "public", nomeTabela);
    
        while (foreignKeyResultSet.next())
        {
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
                                      .append(" (").append(foreignColumnName).append(");\n");
                                    //   .append(" ON DELETE CASCADE ON UPDATE CASCADE;\n");
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
            if( conexao == null) return null;

            DatabaseMetaData conexaoMetaData = conexao.getMetaData();
            ResultSet tabelas = conexaoMetaData.getTables(null, null, "%", new String[] {"TABLE"});
            
            Set<String> nomeTabelas = new HashSet<>();
            
            while (tabelas.next())
            {
                String schema = tabelas.getString("TABLE_SCHEM");
                String nomeTabela = tabelas.getString("TABLE_NAME");

                nomeTabelas.add(schema + "." + nomeTabela);
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
    
    public String compararEstruturaTabela(Connection conexaoCloud, Connection conexaoLocal, 
            String nomeTabela) throws SQLException
    {
        
        StringBuilder alteracoes = new StringBuilder();
        
        Map<String, Coluna> estruturaCloud = obterEstruturaColunas(conexaoCloud, nomeTabela);
        Map<String, Coluna> estruturaLocal = obterEstruturaColunas(conexaoLocal, nomeTabela);
        
        compararColunas(alteracoes, nomeTabela, estruturaCloud, estruturaLocal);
        compararColunasRemovidas(alteracoes, nomeTabela, estruturaCloud, estruturaLocal);
        
        return alteracoes.length() > 0 ?  alteracoes.toString() : null;
    }

    private Map<String, Coluna> obterEstruturaColunas(Connection conexao, String nomeTabela) 
            throws SQLException
    {
        
        Map<String, Coluna> estrutura = new HashMap<>();
        DatabaseMetaData metaData = conexao.getMetaData();
      

        try (ResultSet colunas = metaData.getColumns(null, "public", nomeTabela, null))
        {
            while (colunas.next())
            {
                Coluna coluna = new Coluna();
                coluna.setNome(colunas.getString("COLUMN_NAME"));
                coluna.setNullable(colunas.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
                coluna.setDefaultValor(colunas.getString("COLUMN_DEF"));
                
                String tipoOriginal = colunas.getString("TYPE_NAME").toLowerCase();
                String tipoConvertido = DicionarioTipoSql.getTipoConvertido(tipoOriginal);
                coluna.setTipo(tipoConvertido);


                estrutura.put(coluna.getNome().toLowerCase(), coluna);
            }
        }
        
        return estrutura;
    }

    // Método auxiliar para comparar colunas existentes
    private void compararColunas(StringBuilder alteracoes, String nomeTabela,
            Map<String, Coluna> estruturaCloud, Map<String, Coluna> estruturaLocal)
    {
        
        estruturaCloud.forEach((nomeColuna, colunaCloud) ->
        {
            Coluna colunaLocal = estruturaLocal.get(nomeColuna);
            
            if (colunaLocal == null)
            {
                boolean fl_anulavel = colunaCloud.isNullable();
                String tipo = colunaCloud.getTipo();
                String defaultValor = colunaCloud.getDefaultValor();
    
                //definir valor 
                if (!fl_anulavel && defaultValor == null)
                {
                    defaultValor = obterValorDefault(tipo);
                }
    
                //coluna com DEFAULT
                alteracoes.append(String.format(
                    "ALTER TABLE %s ADD COLUMN %s %s DEFAULT %s;%n",
                    nomeTabela,
                    colunaCloud.getNome(),
                    tipo,
                    defaultValor
                ));
    
                // aplica o NOT NULL 
                if (!fl_anulavel)
                {
                    alteracoes.append(String.format(
                        "ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;%n",
                        nomeTabela,
                        colunaCloud.getNome()
                    ));
                }
            }
            else
            {
                //diferencas
                compararTipoColuna(alteracoes, nomeTabela, nomeColuna, colunaCloud, colunaLocal);
                compararNullableColuna(alteracoes, nomeTabela, nomeColuna, colunaCloud, colunaLocal);
                compararDefaultColuna(alteracoes, nomeTabela, nomeColuna, colunaCloud, colunaLocal);
            }
        });
    }

   
    private void compararTipoColuna(StringBuilder alteracoes, String nomeTabela, String nomeColuna,
            Coluna cloud, Coluna local)
    {
    
        if (!cloud.getTipo().equalsIgnoreCase(local.getTipo()))
        {
            alteracoes.append(String.format(
                "ALTER TABLE %s ALTER COLUMN %s TYPE %s;%n",
                nomeTabela, nomeColuna, cloud.getTipo()
            ));
        }
    }

    private void compararNullableColuna(StringBuilder alteracoes, String nomeTabela, String nomeColuna,
            Coluna cloud, Coluna local)
    {
        if (cloud.isNullable() != local.isNullable())
        {
            alteracoes.append(String.format(
                "ALTER TABLE %s ALTER COLUMN %s %s NOT NULL;%n",
                nomeTabela, nomeColuna, cloud.isNullable() ? "DROP" : "SET"
            ));
        }
    }

    private void compararDefaultColuna(StringBuilder alteracoes, String nomeTabela, String nomeColuna,
            Coluna cloud, Coluna local)
    {
        if (!Objects.equals(cloud.getDefaultValor(), local.getDefaultValor()))
        {
            if (cloud.getDefaultValor() == null)
            {
                alteracoes.append(String.format(
                    "ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;%n",
                    nomeTabela, nomeColuna
                ));
            }
            else
            {
                alteracoes.append(String.format(
                    "ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;%n",
                    nomeTabela, nomeColuna, cloud.getDefaultValor()
                ));
            }
        }
    }

    private void compararColunasRemovidas(StringBuilder alteracoes, String nomeTabela,
            Map<String, Coluna> estruturaCloud, Map<String, Coluna> estruturaLocal)
    {
        
        estruturaLocal.keySet().stream()
            .filter(nomeColuna -> !estruturaCloud.containsKey(nomeColuna))
            .forEach(nomeColuna -> alteracoes.append(String.format(
                "ALTER TABLE %s DROP COLUMN %s;%n",
                nomeTabela, nomeColuna
            )));
    }

    private String obterValorDefault(String tipo)
    {
        switch (tipo.toLowerCase())
        {
            case "boolean":
            case "bool":
                return "false";
            case "int":
            case "integer":
            case "bigint":
            case "smallint":
                return "0";
            case "numeric":
            case "decimal":
            case "float":
            case "double":
                return "0.0";
            case "timestamp":
            case "timestamptz":
                return "'1970-01-01 00:00:00'";
            case "date":
                return "'1970-01-01'";
            case "text":
            case "varchar":
            case "char":
                return "''";
            default:
                return "null";
        }
    }
    

   

}
