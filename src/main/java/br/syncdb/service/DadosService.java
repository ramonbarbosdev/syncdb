package br.syncdb.service;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DadosService
{
    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private CicloService cicloService;
    
    
    public long obterMaxId(Connection conexao, String tabela, String nomeColuna) throws SQLException
    {
       
        if (nomeColuna == null || nomeColuna.isEmpty())
        {
            nomeColuna = obterNomeColunaPK(conexao, tabela);
            if (nomeColuna == null) {
                throw new SQLException("Não foi possível identificar a coluna para obter o máximo ID");
            }
        }

        String sql = String.format("SELECT COALESCE(MAX(%s), 0) FROM %s", nomeColuna, tabela);
        
        try (PreparedStatement stmt = conexao.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery())
            {
            
            if (rs.next())
            {
                return rs.getLong(1);
            }
            return 0;
        }
    }


    public String obterNomeColunaPK(Connection conexao, String tabela) throws SQLException
    {
        try (ResultSet rs = conexao.getMetaData().getPrimaryKeys(null, null, tabela))
        {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
            return null;
        }
    }

    public void cargaInicialCompleta(Connection conexaoCloud, Connection conexaoLocal, String tabela, Map<String, Object> response) throws SQLException
    {
        final int BATCH_SIZE = 1000;
        final int PAGE_SIZE = 50000;
        long offset = 0;
        
        try (java.sql.Statement cloudStmt = conexaoCloud.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        {
            cloudStmt.setFetchSize(BATCH_SIZE);

            while (true)
            {
                String query = String.format("SELECT * FROM %s ORDER BY 1 LIMIT %d OFFSET %d", 
                                              tabela, PAGE_SIZE, offset);

                try (ResultSet rs = cloudStmt.executeQuery(query))
                {
                    if (rs.isBeforeFirst())
                    {
                
                        PreparedStatement localInsert = criarPreparedInsert(conexaoLocal, tabela, rs.getMetaData());

                        int batchCount = 0;
                        conexaoLocal.setAutoCommit(false);

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

                        if (batchCount > 0)
                        {
                            localInsert.executeBatch();
                            conexaoLocal.commit();
                        }

                        localInsert.close();

                        if (batchCount < PAGE_SIZE) break;

                        offset += PAGE_SIZE;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            // response.put("message", "Sincronização da tabela "+tabela+" concluida.");
           System.out.println("Sincronização da tabela "+tabela+" concluida.");

        }
        catch (SQLException e)
        {
            conexaoLocal.rollback();
            throw e;
        }
    }
    
    public void sincronizacaoIncremental(Connection conexaoCloud, Connection conexaoLocal, 
        String tabela,
        String pkColumn,
        long maxLocalId,
        Map<String, Object> response
    ) throws SQLException
    {
        final int BATCH_SIZE = 1000;
        
        String query = String.format("SELECT * FROM %s WHERE %s > ? ORDER BY %s", 
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
                System.out.println("Sincronização da tabela "+tabela+" concluida.");

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

    public Map<String, Set<String>> obterDependenciasTabelas(Connection conexao) throws SQLException
    {
        Map<String, Set<String>> dependencias = new HashMap<>();
        DatabaseMetaData meta = conexao.getMetaData();
        
      
        try (ResultSet tabelas = meta.getTables(null, null, "%", new String[]{"TABLE"}))
        {
            while (tabelas.next()) {
                String nomeTabela = tabelas.getString("TABLE_NAME");
                dependencias.putIfAbsent(nomeTabela, new HashSet<>());
            }
        }
        
        try (ResultSet fks = meta.getImportedKeys(conexao.getCatalog(), null, null))
        {
            while (fks.next()) {
                String tabelaFilha = fks.getString("FKTABLE_NAME");
                String tabelaPai = fks.getString("PKTABLE_NAME");
                dependencias.computeIfAbsent(tabelaFilha, k -> new HashSet<>()).add(tabelaPai);
            }
        }
        
        return dependencias;
    }

    public List<String> ordenarTabelasPorDependencia(Map<String, Set<String>> dependencias)
    {
        List<String> ordenadas = new ArrayList<>();
        Set<String> visitadas = new HashSet<>();
        Set<String> emProcessamento = new HashSet<>();
        Set<Set<String>> ciclos = new HashSet<>();

        cicloService.detectarCiclos(dependencias, ciclos);
        
        for (String tabela : dependencias.keySet())
        {
            if (!visitadas.contains(tabela))
            {
                cicloService.ordenacaoTopologica(tabela, dependencias, visitadas, emProcessamento, ordenadas, ciclos);
            }
        }
        
        return ordenadas;
    }


   
    public void desativarConstraints(Connection conn) throws SQLException
    {
        try (java.sql.Statement stmt = conn.createStatement())
        {
            // Método mais eficiente (desativa todas as constraints de uma vez)
            stmt.execute("SET session_replication_role = replica");
            
            // Alternativa para versões mais antigas:
            // stmt.execute("SET CONSTRAINTS ALL DEFERRED");
        }
    }
    
    public void ativarConstraints(Connection conn) throws SQLException
    {
        try (java.sql.Statement stmt = conn.createStatement())
        {
            // Reativa todas as constraints
            stmt.execute("SET session_replication_role = default");
        }
    }

    public Map<String, Object> validarIntegridadeComRelatorio(Connection conn) 
    {
        Map<String, Object> relatorio = new LinkedHashMap<>();
        List<Map<String, Object>> problemas = new ArrayList<>();
        boolean integridadeOk = true;
        
        try (java.sql.Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT tc.table_name, tc.constraint_name, " +
                "kcu.column_name, ccu.table_name AS foreign_table_name " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                "  ON tc.constraint_name = kcu.constraint_name " +
                "JOIN information_schema.constraint_column_usage ccu " +
                "  ON ccu.constraint_name = tc.constraint_name " +
                "WHERE tc.constraint_type = 'FOREIGN KEY'"))
        {
            
            while (rs.next())
            {
                String table = rs.getString("table_name");
                String constraint = rs.getString("constraint_name");
                String column = rs.getString("column_name");
                String foreignTable = rs.getString("foreign_table_name");
                
                try {
                    // Query para encontrar registros inconsistentes
                    String query = String.format(
                        "SELECT COUNT(*) FROM %s t WHERE NOT EXISTS " +
                        "(SELECT 1 FROM %s ft WHERE t.%s = ft.id)",
                        table, foreignTable, column);
                    
                    try (java.sql.Statement countStmt = conn.createStatement();
                        ResultSet countRs = countStmt.executeQuery(query))
                    {
                        
                        if (countRs.next() && countRs.getInt(1) > 0)
                        {
                            Map<String, Object> problema = new HashMap<>();
                            problema.put("tabela", table);
                            problema.put("constraint", constraint);
                            problema.put("coluna", column);
                            problema.put("tabela_referencia", foreignTable);
                            problema.put("registros_inconsistentes", countRs.getInt(1));
                            problemas.add(problema);
                            integridadeOk = false;
                        }
                    }
                }
                catch (SQLException e)
                {
                    Map<String, Object> erro = new HashMap<>();
                    erro.put("tabela", table);
                    erro.put("erro", "Falha ao validar: " + e.getMessage());
                    problemas.add(erro);
                    integridadeOk = false;
                }
            }
        }
        catch (SQLException e)
        {
            relatorio.put("erro", "Falha ao gerar relatório de integridade: " + e.getMessage());
            return relatorio;
        }
        
        relatorio.put("integridade_ok", integridadeOk);
        relatorio.put("total_problemas", problemas.size());
        relatorio.put("problemas", problemas);
        
        return relatorio;
    }

    public void validarEstruturaTabela(Connection conexaoCloud, Connection conexaoLocal, 
    String tabela) throws SQLException
    {
        if (tabela != null && databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, tabela) != null)
        {
        throw new SQLException("Estrutura da tabela " + tabela + " divergente entre cloud e local");
        }
    }

    public void validarIntegridadeDados(Connection conexaoLocal, Map<String, Object> response) throws SQLException
    {
        Map<String, Object> validacao = validarIntegridadeComRelatorio(conexaoLocal);
        response.put("validacao", validacao);
        
        if (!(Boolean) validacao.getOrDefault("integridade_ok", true))
        {
            throw new SQLException("Problemas de integridade encontrados");
        }
    }

    public List<String> filtrarTabelasRelevantes(String tabelaAlvo, List<String> ordemCarga, Map<String, Set<String>> dependencias)
    {
        if (tabelaAlvo == null) return ordemCarga;
        
        return ordemCarga.stream()
            .filter(t -> t.equals(tabelaAlvo) || 
                        dependencias.getOrDefault(tabelaAlvo, Collections.emptySet()).contains(t))
            .collect(Collectors.toList());
    }

    
    public void processarCargaTabelas(Connection conexaoCloud, Connection conexaoLocal, List<String> tabelas, Map<String, Object> response) throws SQLException
    {
        desativarConstraints(conexaoLocal);

        tabelas.forEach(tabela ->
        {
            try
            {
                String pkColumn = obterNomeColunaPK(conexaoCloud, tabela);
                long maxCloudId = obterMaxId(conexaoCloud, tabela, pkColumn);
                long maxLocalId = obterMaxId(conexaoLocal, tabela, pkColumn);
    
                if (maxLocalId == 0)
                {
                    cargaInicialCompleta(conexaoCloud, conexaoLocal, tabela, response);
                    response.put(tabela + "_status", "CARGA_INICIAL_COMPLETA");
                } 
                else if (maxCloudId > maxLocalId)
                {
                    sincronizacaoIncremental(conexaoCloud, conexaoLocal, tabela, pkColumn, maxLocalId, response);
                    response.put(tabela + "_status", "SINCRONIZACAO_INCREMENTAL");
                } 
                else
                {
                    response.put(tabela + "_status", "JA_SINCRONIZADA");
                    System.out.println("Dados da tabela "+tabela+" já sincronizados" );
                }

                if(tabela.contains("natureza_despesa"))
                {

                    validarTabelaIndividual(conexaoLocal, tabela, response);
                }
            }
            catch (SQLException e)
            {
                response.put(tabela + "_status", "ERRO");
                response.put(tabela + "_erro", e.getMessage());
                System.out.println("Erro ao sincronizar tabela "+tabela );

            }
        });

        validarIntegridadeFinal(conexaoLocal, response);
        
    }


    public void validarTabelaIndividual(Connection conn, String tabela, Map<String, Object> response) {
        try {
            // Verifica apenas as FKs da tabela atual
            Map<String, Object> validacao = validarTabela(conn, tabela);
            response.put(tabela + "_validacao", validacao);
            
            if (!(Boolean) validacao.getOrDefault("integridade_ok", true)) {
                throw new SQLException("Problemas de integridade na tabela " + tabela);
            }
        } catch (SQLException e) {
            System.out.println("Erro na validação da tabela "+tabela );

            throw new RuntimeException(e);
        }
    }



    public Map<String, Object> validarTabela(Connection conn, String tabela) throws SQLException {
        Map<String, Object> resultado = new HashMap<>();
        List<Map<String, String>> problemas = new ArrayList<>();
        boolean integridadeOk = true;
        
        // Obter todas as FKs da tabela
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getImportedKeys(conn.getCatalog(), null, tabela)) {
            while (rs.next()) {
                String fkName = rs.getString("FK_NAME");
                String fkColumn = rs.getString("FKCOLUMN_NAME");
                String pkTable = rs.getString("PKTABLE_NAME");
                String pkColumn = rs.getString("PKCOLUMN_NAME");
                
                try {
                    // Verificar registros inconsistentes
                    String query = String.format(
                        "SELECT COUNT(*) FROM %s t WHERE t.%s IS NOT NULL " +
                        "AND NOT EXISTS (SELECT 1 FROM %s r WHERE t.%s = r.%s)", 
                        tabela, fkColumn, pkTable, fkColumn, pkColumn);
                    
                    try (java.sql.Statement stmt = conn.createStatement();
                         ResultSet countRs = stmt.executeQuery(query)) {
                        
                        if (countRs.next() && countRs.getInt(1) > 0) {
                            Map<String, String> problema = new HashMap<>();
                            problema.put("constraint", fkName);
                            problema.put("coluna", fkColumn);
                            problema.put("tabela_referencia", pkTable);
                            problema.put("registros_inconsistentes", String.valueOf(countRs.getInt(1)));
                            problemas.add(problema);
                            integridadeOk = false;
                        }
                    }
                } catch (SQLException e) {
                    Map<String, String> erro = new HashMap<>();
                    erro.put("erro", "Falha ao validar FK " + fkName + ": " + e.getMessage());
                    problemas.add(erro);
                    integridadeOk = false;
                }
            }
        }
        
        resultado.put("integridade_ok", integridadeOk);
        resultado.put("problemas", problemas);
        return resultado;
    }
    
    private void validarIntegridadeFinal(Connection conn, Map<String, Object> response) throws SQLException {
        try
        {
            ativarConstraints(conn);

         
            Map<String, Object> validacao = validarIntegridadeComRelatorio(conn);

            response.put("validacao_final", validacao);
            
            if (!(Boolean) validacao.getOrDefault("integridade_ok", true)) {
                throw new SQLException("Problemas de integridade encontrados após carga completa");
            }
        } catch (SQLException e) {
            System.out.println("Erro na validação final "+e );
            throw e;
        }
    }
    

}
