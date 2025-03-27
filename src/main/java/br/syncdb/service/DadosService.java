package br.syncdb.service;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
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


import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.persistence.criteria.CriteriaBuilder;

@Service
public class DadosService
{
    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private CicloService cicloService;

    @Autowired
    private EstruturaService estruturaService;
    
    
    public long obterMaxId(Connection conexao, String tabela, String nomeColuna) throws SQLException
    {
    
        DSLContext create = DSL.using(conexao, SQLDialect.POSTGRES);

        if (nomeColuna == null || nomeColuna.isEmpty()) {
            nomeColuna = obterNomeColunaPK(conexao, tabela);
            if (nomeColuna == null) {
                throw new SQLException("Não foi possível identificar a coluna para obter o máximo ID");
            }
        }
    
        // Montando a query com jOOQ (SQLBuilder)
        Long maxId = create
                    .select(DSL.coalesce(DSL.max(DSL.field(nomeColuna, Long.class)), 0L))
                    .from(DSL.table(tabela))
                    .fetchOneInto(Long.class);

        
        return maxId != null ? maxId : 0;
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
    public int obterQuantidadeRegistro(Connection conexao, String tabela) throws SQLException
    {
        DSLContext create = DSL.using(conexao, SQLDialect.POSTGRES);

        int count = create.fetchCount(DSL.table(tabela));
        
        return count;

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
            stmt.execute("SET session_replication_role = origin");
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

    public List<String> filtrarTabelasRelevantes(String tabela, List<String> ordemCarga, 
    Map<String, Set<String>> dependencias)
    {
        Set<String> tabelasRelevantes = new LinkedHashSet<>();
        Set<String> visitado = new HashSet<>();
        Set<String> ciclo = new HashSet<>();

        buscarDependencias(tabela, dependencias, tabelasRelevantes, visitado, ciclo);

        return ordemCarga.stream()
                        .filter(tabelasRelevantes::contains)
                        .collect(Collectors.toList());

        }

        private void buscarDependencias(String tabela, Map<String, Set<String>> dependencias, 
        Set<String> tabelasRelevantes, Set<String> visitado, Set<String> ciclo)
        {

            if (ciclo.contains(tabela))
            {
                System.out.println("Ciclo detectado em: " + tabela);

                dependencias.getOrDefault(tabela, new HashSet<>()).clear();
                // return;
            }
            if (visitado.contains(tabela))
            {
                return;
            }

            visitado.add(tabela);
            tabelasRelevantes.add(tabela);

            if (dependencias.containsKey(tabela))
            {
                for (String dependente : dependencias.get(tabela))
                {
                    buscarDependencias(dependente, dependencias, tabelasRelevantes, visitado, ciclo);
                }
            }
        }

    
    public void processarCargaTabelas(Connection conexaoCloud, Connection conexaoLocal, List<String> tabelas, Map<String, Object> response) throws SQLException
    {
        tabelas.forEach(tabela ->
        {
            try
            {
                String pkColumn = obterNomeColunaPK(conexaoCloud, tabela);
                long maxCloudId = obterMaxId(conexaoCloud, tabela, pkColumn);
                long maxLocalId = obterMaxId(conexaoLocal, tabela, pkColumn);
                int countCloud = obterQuantidadeRegistro(conexaoCloud, tabela);
                int countLocal= obterQuantidadeRegistro(conexaoLocal, tabela);

                if (maxLocalId == 0)
                {
                    cargaInicialCompleta(conexaoCloud, conexaoLocal, tabela, response);
                    response.put(tabela + "_status", "CARGA_INICIAL_COMPLETA");
                    System.out.println("Sincronização da tabela "+tabela+" concluida.");

                } 
                else if (maxCloudId > maxLocalId || countCloud > countLocal)
                {
                    sincronizacaoIncremental(conexaoCloud, conexaoLocal, tabela, pkColumn, maxLocalId, response);
                    verificarConsistenciaRegistros(conexaoLocal, conexaoCloud, tabela, pkColumn);
                    response.put(tabela + "_status", "SINCRONIZACAO_INCREMENTAL");
                    System.out.println("Sincronização da tabela "+tabela+" concluida.");

                } 
                else
                {
                    response.put(tabela + "_status", "JA_SINCRONIZADA");
                    System.out.println("Dados da tabela "+tabela+" já sincronizados" );
                }

                // validarTabelaIndividual(conexaoLocal, tabela, response);
            }
            catch (SQLException e)
            {
                response.put(tabela + "_status", "ERRO");
                response.put(tabela + "_erro", e.getMessage());
                System.out.println("Erro ao sincronizar tabela "+tabela );

            }
        });

        // validarIntegridadeFinal(conexaoLocal, response);
        
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
        
        DatabaseMetaData meta = conn.getMetaData();
        String catalog = conn.getCatalog();
        
        try (ResultSet rs = meta.getImportedKeys(catalog, null, tabela)) {
            while (rs.next()) {
                String fkName = rs.getString("FK_NAME");
                String fkColumn = rs.getString("FKCOLUMN_NAME");
                String pkTable = rs.getString("PKTABLE_NAME");
                String pkColumn = rs.getString("PKCOLUMN_NAME");
                
                try {
                    // Usa JOIN para melhorar performance
                    String query = String.format(
                        "SELECT COUNT(*) FROM %s t LEFT JOIN %s r ON t.%s = r.%s " +
                        "WHERE t.%s IS NOT NULL AND r.%s IS NULL", 
                        tabela, pkTable, fkColumn, pkColumn, fkColumn, pkColumn);
                    
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
    
                            // Mostra os registros inconsistentes para debug
                            String queryDetalhada = String.format(
                                "SELECT t.%s FROM %s t LEFT JOIN %s r ON t.%s = r.%s " +
                                "WHERE t.%s IS NOT NULL AND r.%s IS NULL LIMIT 5", 
                                fkColumn, tabela, pkTable, fkColumn, pkColumn, fkColumn, pkColumn);
    
                            try (ResultSet rsDetalhado = stmt.executeQuery(queryDetalhada)) {
                                while (rsDetalhado.next()) {
                                    System.out.println("Inconsistência encontrada: " + rsDetalhado.getString(1));
                                }
                            }
                        }
                    }
                } catch (SQLException e) {
                    Map<String, String> erro = new HashMap<>();
                    erro.put("erro", String.format("Falha ao validar FK '%s' (coluna '%s' -> tabela '%s'): %s", fkName, fkColumn, pkTable, e.getMessage()));
                    problemas.add(erro);
                    integridadeOk = false;
                }
            }
        }
        
        resultado.put("integridade_ok", integridadeOk);
        resultado.put("problemas", problemas);
        return resultado;
    }
    

    public  Result<Record1<Object>> verificarConsistenciaRegistros(Connection conexaoLocal, Connection conexaoCloud, String tabela, String pkColumn) throws SQLException 
    {
   
        DSLContext createLocal = DSL.using(conexaoLocal, SQLDialect.POSTGRES);
        DSLContext createCloud = DSL.using(conexaoCloud, SQLDialect.POSTGRES);

        Set<Long> registrosLocal = new HashSet<>();
        Set<Long> registrosCloud = new HashSet<>();

        Result<Record1<Object>>  sqlLocal = createLocal
                          .select(DSL.field(pkColumn))
                          .from(DSL.table(tabela))
                          .fetch();

        Result<Record1<Object>> sqlCloud = createCloud
                                            .select(DSL.field(pkColumn))
                                            .from(DSL.table(tabela))
                                            .fetch();


        if(sqlLocal == null)
         {
            return null;
        }
        
        for (Record1<Object> local : sqlLocal)
        {
            registrosLocal.add(((Number) local.getValue(pkColumn)).longValue());
        }

        for(Record1<Object> cloud : sqlCloud)
        {
            registrosCloud.add(((Number) cloud.getValue(pkColumn)).longValue());
        }

        Set<Long> registrosDesconhecidos = new HashSet<>(registrosLocal);
        registrosDesconhecidos.removeAll(registrosCloud);
            
        if (!registrosDesconhecidos.isEmpty())
        {
                System.out.println("Registros não encontrados na base de dados remota: " + registrosDesconhecidos);
        }

        Set<Long> registrosExtras = new HashSet<>(registrosCloud);
        registrosExtras.removeAll(registrosLocal);
        
        if (!registrosExtras.isEmpty())
        {
            System.out.println("Registros extras na base de dados remota: " + registrosExtras);
        }
        
        return sqlLocal;
     
       
    }

    public void verificarConsistenciaDados(Connection conexaoLocal, Connection conexaoCloud, String tabela, String pkColumn) throws SQLException {
        // 1. Obtenha os registros das duas bases de dados
        String sqlLocal = String.format("SELECT * FROM %s WHERE %s = ?", tabela, pkColumn);
        String sqlCloud = String.format("SELECT * FROM %s WHERE %s = ?", tabela, pkColumn);
    
        try (PreparedStatement stmtLocal = conexaoLocal.prepareStatement(sqlLocal);
             PreparedStatement stmtCloud = conexaoCloud.prepareStatement(sqlCloud)) {
    
            // Supondo que o PK seja único, então pegamos o ID
            stmtLocal.setLong(1, 123);  // Exemplo: ID de registro
            stmtCloud.setLong(1, 123);
    
            try (ResultSet rsLocal = stmtLocal.executeQuery();
                 ResultSet rsCloud = stmtCloud.executeQuery()) {
    
                // Verifica se o registro existe nas duas bases
                if (rsLocal.next() && rsCloud.next()) {
                    // Comparar todos os valores das colunas
                    ResultSetMetaData rsMetaDataLocal = rsLocal.getMetaData();
                    ResultSetMetaData rsMetaDataCloud = rsCloud.getMetaData();
    
                    int columnCount = rsMetaDataLocal.getColumnCount();
    
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rsMetaDataLocal.getColumnName(i);
                        String localValue = rsLocal.getString(i);
                        String cloudValue = rsCloud.getString(i);
    
                        if (!localValue.equals(cloudValue)) {
                            System.out.println("Diferença encontrada na coluna: " + columnName + 
                                               " | Base Local: " + localValue + " | Base Remota: " + cloudValue);
                        }
                    }
                } else {
                    System.out.println("Registro com ID " + 123 + " não encontrado em uma das bases.");
                }
            }
        }
    }
    
    
    

}
