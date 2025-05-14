package br.syncdb.service;

import java.beans.Statement;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.SQLDataType;
import org.jooq.util.postgres.PGobject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import br.syncdb.model.EstruturaTabela;
import br.syncdb.model.TabelaDetalhe;


@Service
public class OperacaoBancoService
{

    @Autowired
    private ProcessoService processoService;

    public List<String> registroDesconhecido(Connection connection, String tabela, Long id, String pkColumn )
    {
        DSLContext dsl = DSL.using(connection);

        Table<Record> tabelaRecord = DSL.table(tabela);

        List<String>  queryList = new ArrayList<>();
        
        String deleteQuery = dsl.delete(tabelaRecord)
                                .where(DSL.field(pkColumn).eq(id))
                                .getSQL(ParamType.INLINED)
                                .toString();

        queryList.add(deleteQuery);
        // queryList.add(";\n");

        return queryList;
        
    }

    public List<String> registroExtra(Connection conexaoLocal, Connection conexaoCloud, String tabela, Long id, String pkColumn) throws SQLException {
        List<String> queryList = new ArrayList<>();

        String sql = "SELECT * FROM " + tabela + " WHERE " + pkColumn + " = ?";
        try (PreparedStatement stmt = conexaoCloud.prepareStatement(sql)) {
            stmt.setLong(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String insertSQL = construirInsertSQL(tabela, rs);
                    queryList.add(insertSQL);
                } else {
                    throw new SQLException("Nenhum dado encontrado na tabela '" + tabela + "' com ID " + id);
                }
            }
        }

        return queryList;
    }


    public void executarQueriesEmLotes(Connection conexao, HashMap<String, List<String>> queries, List<Map<String, String>> detalhes) {
        try
        {
            int totalQueries = queries.values().stream().mapToInt(List::size).sum();
            AtomicInteger queriesExecutadas = new AtomicInteger(0);
        
            // Deixar o commit manual
            conexao.setAutoCommit(false);
        
            for (Map.Entry<String, List<String>> grupo : queries.entrySet())
            {
                String tipo = grupo.getKey();
                List<String> listaQueries = grupo.getValue();
    
                executarGrupoDeQueries(conexao, tipo, listaQueries, detalhes, totalQueries, queriesExecutadas);
                conexao.commit();

            }
    
            processoService.enviarProgresso("Concluido", 100, "Processamento concluído com sucesso", null);
    
        } catch (SQLException e) {
            System.err.println("Falha na transação geral: " + e.getMessage());
            try {
                conexao.rollback();
            } catch (SQLException ex) {
                System.err.println("Erro ao tentar rollback: " + ex.getMessage());
            }
        } finally {
            try {
                conexao.setAutoCommit(true);
            } catch (SQLException e) {
                System.err.println("Erro ao reativar autoCommit: " + e.getMessage());
            }
        }
    }

    private void executarGrupoDeQueries(Connection conexao, String tipo, List<String> queries, List<Map<String, String>> detalhes, int totalQueries, AtomicInteger queriesExecutadas) throws SQLException {
        if (queries == null || queries.isEmpty()) return;

        System.out.println("\n=== Executando grupo: " + tipo + " ===");

        if(tipo.contains("Chaves Estrangeiras"))
        {
            System.out.println(tipo);
        }
    
        for (String query : queries)
        {
            String tabela = extrairNomeTabelaDaQuery(query);
    
            int progressoAtual = (int) ((queriesExecutadas.incrementAndGet() / (double) totalQueries) * 100);
            processoService.enviarProgresso("Processando", progressoAtual, "Processando " + tipo + ": " + tabela, tabela);
    
            try (java.sql.Statement stmt = conexao.createStatement())
            {
                stmt.execute(query);
            }
            catch (SQLException e)
            {
                Map<String, String> criarDetalhe = new LinkedHashMap<>();
                criarDetalhe.put("tabela", tabela);
                criarDetalhe.put("acao", tipo);
                criarDetalhe.put("erro", e.getMessage() + " | SQLState: " + e.getSQLState());
                detalhes.add(criarDetalhe);
    
                throw e; 
            }
        }
        
    
        conexao.commit();

    }
    
    
    public String extrairNomeTabelaDaQuery(String query) {
        query = query.trim().toUpperCase();
    
        String patternCreate = "CREATE TABLE IF NOT EXISTS ([\\w\\.]+)";
        String patternCreateSimple = "CREATE TABLE ([\\w\\.]+)";
        String patternAlter = "ALTER TABLE ([\\w\\.]+)";
        String patternForeign = "ALTER TABLE ONLY ([\\w\\.]+)";
        String patternInsert = "INSERT INTO ([\\w\\.]+)";
        
        List<String> patterns = Arrays.asList(patternCreate, patternCreateSimple, patternAlter, patternForeign, patternInsert);
    
        for (String patternStr : patterns)
        {
            Pattern pattern = Pattern.compile(patternStr);
            Matcher matcher = pattern.matcher(query);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
    
        return "desconhecida"; // Caso não consiga identificar
    }
    

    public List<String> cargaInicialCompleta(Connection conexaoCloud, Connection conexaoLocal, String tabela) throws SQLException {
        // Map para armazenar as instruções SQL (cache)
        List<String> sqlCache = new ArrayList<>();

        final int BATCH_SIZE = 1000;
        final int PAGE_SIZE = 50000;
        long offset = 0;

        try (java.sql.Statement cloudStmt = conexaoCloud.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            cloudStmt.setFetchSize(BATCH_SIZE);

            while (true) {
                String query = String.format("SELECT * FROM %s ORDER BY 1 LIMIT %d OFFSET %d", tabela, PAGE_SIZE, offset);

                try (ResultSet rs = cloudStmt.executeQuery(query)) {
                    if (rs.isBeforeFirst()) {
                        // Coletar os registros e construir as instruções SQL
                        while (rs.next()) {
                            // Supondo que a primeira coluna seja a chave (id)
                            String id = rs.getString(1);
                            // Construir a instrução SQL
                            String sql = construirInsertSQL(tabela, rs);
                            // Armazenar a instrução SQL no Map
                            sqlCache.add(sql);
                        }

                        // Verificar se terminou a paginação
                        if (sqlCache.size() < PAGE_SIZE) {
                            break;
                        }

                        offset += PAGE_SIZE;
                    } else {
                        break;
                    }
                }
            }
        
        }
        catch (BatchUpdateException e)
        {
            conexaoLocal.rollback();
            handleBatchUpdateException(e, tabela);
        }
        catch (SQLException e)
        {
            conexaoLocal.rollback();
            throw e;
        }

        // Retornar o Map com todas as instruções SQL
        return sqlCache;
    }

    // Método para construir a instrução SQL de INSERT
    private String construirInsertSQL(String tabela, ResultSet rs) throws SQLException
    {
        // if(!tabela.equals("item_estrutura_operacao")) return null;
       

        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tabela).append(" (");
    
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
    
        // Adicionar os nomes das colunas
        for (int i = 1; i <= columnCount; i++)
        {
            sql.append(metaData.getColumnName(i));
            if (i < columnCount) {
                sql.append(", ");
            }
        }
    
        sql.append(") VALUES (");
    
        // Adicionar os valores das colunas
        for (int i = 1; i <= columnCount; i++) {
            Object value = rs.getObject(i);
            String columnTypeName = metaData.getColumnTypeName(i);

            if (value == null)
            {
                sql.append("NULL"); 
            }
            else if (value instanceof String)
            {
             
                sql.append("'").append(escapeApostrophe(value.toString())).append("'");
            }
            else if (value instanceof java.util.Date)
            {
              
                sql.append("'").append(new java.sql.Timestamp(((java.util.Date) value).getTime())).append("'");
            }
            else if (value instanceof org.postgresql.util.PGobject && "jsonb".equals(columnTypeName))
            {
                org.postgresql.util.PGobject pg = (org.postgresql.util.PGobject) value;
                sql.append("'").append(escapeApostrophe(pg.getValue())).append("'::jsonb");
            }
            else
            {
                sql.append(value);
            }

            if (i < columnCount)
            {
                sql.append(", ");
            }
        }
    
        sql.append(");");
    
        String sqlString = sql.length() > 0 ?  sql.toString() : "" ;

        return sqlString;
    }

    private String escapeApostrophe(String value)
    {
        return value.replace("'", "''");
    }

    // Método para tratar exceções de lote
    private void handleBatchUpdateException(BatchUpdateException e, String tabela) {
        System.err.println("Erro durante a execução do lote: " + e.getMessage());

        SQLException nextException = e.getNextException();
        while (nextException != null) {
            if (nextException.getMessage().contains("duplicate key value violates unique constraint")) {
                System.err.println("Erro: Chave duplicada detectada. Registro já existe na tabela " + tabela + ".");
                throw new RuntimeException("Erro: Chave duplicada detectada. Registro já existe na tabela " + tabela + ".");
            } else {
                System.err.println("Outro erro SQL: " + nextException.getMessage());
            }
            nextException = nextException.getNextException();
        }
        throw new RuntimeException("Falha ao executar batch", e);
    }


    
    public void sincronizacaoIncremental(Connection conexaoCloud, Connection conexaoLocal, 
        String tabela,
        String pkColumn,
        long maxLocalId,
        Map<String, Object> response
    ) throws SQLException
    {
        final int BATCH_SIZE = 1000;
        
        // String query = String.format("SELECT * FROM %s WHERE %s > ? ORDER BY %s", 
        //                         tabela, pkColumn, pkColumn);
        String query = String.format("SELECT * FROM %s WHERE %s = ? ORDER BY %s", 
                                tabela, pkColumn, pkColumn);
        
        try (PreparedStatement cloudStmt = conexaoCloud.prepareStatement(query,  ResultSet.TYPE_FORWARD_ONLY,  ResultSet.CONCUR_READ_ONLY))
        {
            cloudStmt.setFetchSize(1000);
            cloudStmt.setLong(1, maxLocalId);
            
            try (ResultSet rs = cloudStmt.executeQuery();  PreparedStatement localInsert = criarPreparedInsert(conexaoLocal, tabela, rs.getMetaData()))
            {
                
                conexaoLocal.setAutoCommit(false);
                int batchCount = 0;
                
                while (rs.next())
                {
                    preencherPreparedStatement(localInsert, rs);
                    localInsert.addBatch();
                    
                    if (++batchCount % BATCH_SIZE == 0)
                    {
                        localInsert.executeBatch();
                        conexaoLocal.commit();
                    }
                }
                
                localInsert.executeBatch();
                conexaoLocal.commit();
                // response.put("message", "Sincronização incremental concluida.");

            }
        }
    }

    
    public PreparedStatement criarPreparedInsert(Connection conexao, String tabela, ResultSetMetaData meta) throws SQLException
    {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tabela).append(" (");
        StringBuilder values = new StringBuilder("VALUES (");
        
        int columnCount = meta.getColumnCount();
        for (int i = 1; i <= columnCount; i++)
        {
            if (i > 1)
            {
                sql.append(", ");
                values.append(", ");
            }
            sql.append(meta.getColumnName(i));
            values.append("?");
        }
        
        sql.append(") ").append(values).append(")");
        
        return conexao.prepareStatement(sql.toString());
    }


    public void preencherPreparedStatement(PreparedStatement stmt, ResultSet rs) throws SQLException
    {
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
        {
            stmt.setObject(i, rs.getObject(i));
        }
    }


}
