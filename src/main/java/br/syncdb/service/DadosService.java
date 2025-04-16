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
import java.util.concurrent.atomic.AtomicInteger;
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

import br.syncdb.config.ConexaoBanco;
import br.syncdb.controller.TipoConexao;
import br.syncdb.model.TabelaDetalhe;
import br.syncdb.utils.UtilsSync;
import jakarta.persistence.criteria.CriteriaBuilder;

@Service
public class DadosService
{
    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private CicloService cicloService;

    @Autowired
    private ProcessoService processoService;

    @Autowired
    private OperacaoBancoService operacaoBancoService;

    @Autowired
    private UtilsSync utilsSync;

    @Autowired
    private CacheService cacheService;



     public Map<String, Object> verificarDados(String base, String tabela)
    {
        Connection conexaoCloud = null;
        Connection conexaoLocal = null;
        Map<String, Object> response = new HashMap<String, Object>();
        List<TabelaDetalhe> detalhes = new ArrayList<>();
       
        try
        {
            conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            HashMap<String, List<String>> querys = obterDadosTabela(conexaoCloud,conexaoLocal, tabela, detalhes );

            cacheService.salvarCache(base + "_dados:", querys);

            response.put("sucesso", true); 
            response.put("mensagem", "Dados Sincronizado.");
            response.put("tabelas_afetadas", detalhes); 

        }
        catch (Exception e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        }
        
        return response;
    }
    public Map<String, Object> sincronizarDados(String base, String tabela, Boolean fl_verificacao)
    {
       
        Connection conexaoLocal = null;
        Map<String, Object> response = new HashMap<String, Object>();
        List<Map<String, String>> detalhes = new ArrayList<>();
       
        try
        {
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);
            conexaoLocal.setAutoCommit(false);

            desativarConstraints(conexaoLocal);

            @SuppressWarnings("unchecked")
            HashMap<String, List<String>> querys = cacheService.buscarCache(base + "_dados:", HashMap.class);

            if (querys == null)
            {
                response.put("sucesso", false);
                response.put("mensagem", "Nenhuma verificação foi feita previamente.");
                return response;
            }
            
            operacaoBancoService.executarQueriesEmLotes(conexaoLocal, querys, detalhes);
            
            response.put("sucesso", true); 
            response.put("mensagem", "Sincronização de dados concluida"); 
            response.put("tabelas_afetadas", detalhes); 

            ativarConstraints(conexaoLocal);

        }
        catch (Exception e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        }
        finally
        {
            ConexaoBanco.fecharTodos();
        }
        
        return response;
    }

    
    private void    tratarErroSincronizacao(Map<String, Object> response, Connection conexaoLocal, Exception e)
    {        
        String errorType = e.getClass().getSimpleName();
        String details = e.getMessage();

        response.put("sucesso", false);
        response.put("erro",errorType);
        response.put("mensagem", "Erro durante sincronização");
        response.put("detalhes", details);
    }
    
    
    public long obterMaxId(Connection conexao, String tabela, String nomeColuna) throws SQLException
    {
        if (nomeColuna == null || nomeColuna.isEmpty())
        {
            nomeColuna = obterNomeColunaPK(conexao, tabela);
            if (nomeColuna == null)
            {
                return (Long) null;
            }
        }

        String tabelaSemSchema = utilsSync.extrairTabela(tabela);

        String sqlCheck = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)";

        try (PreparedStatement stmtCheck = conexao.prepareStatement(sqlCheck))
        {
            stmtCheck.setString(1, tabelaSemSchema);
            try (ResultSet rsCheck = stmtCheck.executeQuery())
            {
                if (rsCheck.next() && !rsCheck.getBoolean(1))
                {
                    throw new SQLException("A tabela '" + tabela + "' não existe.");
                }
            }
        }
        
        String sql = String.format(
            "SELECT COALESCE(MAX(CASE WHEN %s::TEXT ~ '^[0-9]+$' THEN %s::BIGINT ELSE NULL END), 0) FROM %s", 
            nomeColuna, nomeColuna, tabela);
        
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
        String schema = utilsSync.extrairSchema(tabela); 
        String nomeTabela = utilsSync.extrairTabela(tabela); 

        try (ResultSet rs = conexao.getMetaData().getPrimaryKeys(null, schema, nomeTabela))
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

    

    public Map<String, Set<String>> obterDependenciasTabelas(Connection conexao) throws SQLException {
        Map<String, Set<String>> dependencias = new HashMap<>();
        DatabaseMetaData meta = conexao.getMetaData();
    
        // Mapeia todas as tabelas com schema
        try (ResultSet tabelas = meta.getTables(null, null, "%", new String[]{"TABLE"})) {
            while (tabelas.next()) {
                String schema = tabelas.getString("TABLE_SCHEM");
                String nomeTabela = tabelas.getString("TABLE_NAME");
                String chave = schema + "." + nomeTabela;
                dependencias.putIfAbsent(chave, new HashSet<>());
            }
        }
    
        // Mapeia as dependências (FK -> PK)
        try (ResultSet fks = meta.getImportedKeys(conexao.getCatalog(), null, null)) {
            while (fks.next()) {
                String schemaFilha = fks.getString("FKTABLE_SCHEM");
                String tabelaFilha = fks.getString("FKTABLE_NAME");
    
                String schemaPai = fks.getString("PKTABLE_SCHEM");
                String tabelaPai = fks.getString("PKTABLE_NAME");
    
                String chaveFilha = schemaFilha + "." + tabelaFilha;
                String chavePai = schemaPai + "." + tabelaPai;
    
                dependencias.computeIfAbsent(chaveFilha, k -> new HashSet<>()).add(chavePai);
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

    public Map<String, Object> carregarOrdemTabela(Connection conexaoCloud, Connection conexaoLocal, 
    String tabela) throws SQLException
    {
        System.out.println("Iniciando verificação de dados...");

        Map<String, Object> parametrosMap = new HashMap<String, Object>();

        Map<String, Set<String>> dependencias = obterDependenciasTabelas(conexaoCloud);

        List<String> ordemCarga = ordenarTabelasPorDependencia(dependencias);

        if (tabela != null)
        {
            ordemCarga = filtrarTabelasRelevantes(tabela, ordemCarga, dependencias);
        }

        parametrosMap.put("ordemCarga", ordemCarga);

        return parametrosMap;
    }


   
    public void desativarConstraints(Connection conn) throws SQLException
    {
        try (java.sql.Statement stmt = conn.createStatement())
        {    
            stmt.execute("SET session_replication_role = replica");
        }
    }
    
    public void ativarConstraints(Connection conn) throws SQLException
    {
        try (java.sql.Statement stmt = conn.createStatement())
        {
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

    public   Map<String, Object> definirParametrosVerificacao(Connection conexaoCloud, Connection conexaoLocal, String  tabela) throws SQLException
    {

        String pkColumn =  obterNomeColunaPK(conexaoCloud, tabela);

        Map<String, Object> parametros = new HashMap<String, Object>();

        if(tabela == null)
        {
            throw new SQLException("Tabela não especificada.");
        }

        if(pkColumn == null)
        {
           return null;
        }

        // estruturaService.validarEstruturaTabela(conexaoCloud, conexaoLocal, tabela);
        
        long maxCloudId = obterMaxId(conexaoCloud, tabela, pkColumn);
        long maxLocalId = obterMaxId(conexaoLocal, tabela, pkColumn);
        int countCloud = obterQuantidadeRegistro(conexaoCloud, tabela);
        int countLocal= obterQuantidadeRegistro(conexaoLocal, tabela);

        if((Long) maxCloudId == null)
        {
            return null;
        }

        if(maxLocalId == 0 && countLocal == 0)
        {
            parametros.put("novo", true);
        }
        else
        {
            parametros.put("novo", false );
        }

        if(maxCloudId > maxLocalId || countCloud != countLocal)
        {
            parametros.put("existente", true);
        }
        else
        {
            parametros.put("existente", false);
        }

        return parametros;
    }

    public  HashMap<String, List<String>>  obterDadosTabela(Connection conexaoCloud, Connection conexaoLocal, String tabela, List<TabelaDetalhe> detalhes) throws SQLException
    {

        Map<String, Object> parametrosMap = carregarOrdemTabela(conexaoCloud, conexaoLocal, tabela);
        List<String> tabelas = (List<String>) parametrosMap.get("ordemCarga");

        //querys
        List<String> criacaoAtualizacaoSeq = Collections.synchronizedList(new ArrayList<>());
        List<String> criacaoDados = Collections.synchronizedList(new ArrayList<>());
        List<String> atualizacaoDados = Collections.synchronizedList(new ArrayList<>());

        //Processamento
        int totalTabelas = tabelas.size();
        AtomicInteger tabelasProcessadas = new AtomicInteger(0);
        processoService.enviarProgresso("Iniciando", 0, "Iniciando processam de " + totalTabelas + " tabelas", null);
        

        // if(tabelas.size() == 0)  throw new SQLException("Tabela "+tabela+" não encontrada.");
        
        for(String itemTabela : tabelas)
        {    

            //Processamento
            int progresso = (int) ((tabelasProcessadas.incrementAndGet() / (double) totalTabelas) * 100);
            processoService.enviarProgresso("Processando", progresso, "Processando tabela: " + itemTabela, itemTabela);

            Map<String, Object> parametros = definirParametrosVerificacao(conexaoCloud, conexaoLocal, itemTabela);

            if(parametros != null)
            {
                TabelaDetalhe infoDetalhe = new TabelaDetalhe();

                String querySeq= atualizarSequencias(conexaoLocal, itemTabela);
                if (querySeq != null)  criacaoAtualizacaoSeq.add(querySeq);

                if ((Boolean) parametros.get("novo"))
                {
                    System.out.println("Criacao da script da '"+itemTabela+"'.");
                   
                    List<String> query = operacaoBancoService.cargaInicialCompleta( conexaoCloud,  conexaoLocal, itemTabela) ;

                    if(query.size() > 0)
                    {
                        infoDetalhe.setTabela(itemTabela);
                        infoDetalhe.setAcao("Inserção");
                        infoDetalhe.setLinhaInseridas(query.size());
                        detalhes.add(infoDetalhe);
                        criacaoDados.addAll(query);
                    }
                   
                } 
                else if ((Boolean) parametros.get("existente"))
                {
                  
                    System.out.println("Tabela '"+itemTabela+"' com atualizações de dados pendendes.");
                    String pkColumn =  obterNomeColunaPK(conexaoCloud, itemTabela);
                    List<String> query = verificarConsistenciaRegistros(conexaoLocal, conexaoCloud, itemTabela, pkColumn);  

                    if(query.size() > 0)
                    {
                        infoDetalhe.setTabela(itemTabela);
                        infoDetalhe.setAcao("Atualização");
                        infoDetalhe.setLinhaAtualizadas(query.size());
                        detalhes.add(infoDetalhe);
                        atualizacaoDados.addAll(query);
                    }
                } 
                else
                {
                    System.out.println("Tabela '"+itemTabela+"' não possui atualizações de dados pendentes.");
                }
                
            }
    
        }
       
        //Processamento
        processoService.enviarProgresso("Concluido", 100, "Processamento concluído com sucesso", null);

        HashMap<String, List<String>> queries = new LinkedHashMap<>();
        queries.put("Sequencia", criacaoAtualizacaoSeq);
        queries.put("Criacao", criacaoDados);
        queries.put("Atualizacao", atualizacaoDados);
        
        return queries;    
    }
  

    public  List<String> verificarConsistenciaRegistros(Connection conexaoLocal, Connection conexaoCloud, String tabela, String pkColumn) throws SQLException 
    {
        Long id;

        Map<String,  List<String>> resutadoQuery = new LinkedHashMap<>(); 
        resutadoQuery.put("delete", new ArrayList<>());
        resutadoQuery.put("insert", new ArrayList<>());

        List<String> sqlCache = new ArrayList<>();


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

        if(sqlLocal == null || sqlCloud == null)
        {
            return null ;
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
            // System.out.println("Registros desconhecido na base de dados remota, ID: " + registrosDesconhecidos);
            id = registrosDesconhecidos.iterator().next() ;
            resutadoQuery.put("delete", operacaoBancoService.registroDesconhecido( conexaoLocal,  tabela, id,  pkColumn ));
            sqlCache.addAll( operacaoBancoService.registroDesconhecido( conexaoLocal,  tabela, id,  pkColumn ));

        }
        
        Set<Long> registrosExtras = new HashSet<>(registrosCloud);
        registrosExtras.removeAll(registrosLocal);
        
        if (!registrosExtras.isEmpty())
        {
            // System.out.println("Registros extras na base de dados remota, ID: " + registrosExtras);
            id = registrosExtras.iterator().next() ;
            sqlCache.addAll(operacaoBancoService.registroExtra( conexaoLocal, conexaoCloud, tabela, id,  pkColumn ));

        }
        
        return sqlCache;
     
    }

    public void verificarConsistenciaDados(Connection conexaoLocal, Connection conexaoCloud, String tabela, String pkColumn) throws SQLException {
        // 1. Obtenha os registros das duas bases de dados
        String sqlLocal = String.format("SELECT * FROM %s WHERE %s = ?", tabela, pkColumn);
        String sqlCloud = String.format("SELECT * FROM %s WHERE %s = ?", tabela, pkColumn);
    
        try (PreparedStatement stmtLocal = conexaoLocal.prepareStatement(sqlLocal);
             PreparedStatement stmtCloud = conexaoCloud.prepareStatement(sqlCloud))
        {
    
            // Supondo que o PK seja único, então pegamos o ID
            stmtLocal.setLong(1, 123); 
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
    
    public String atualizarSequencias(Connection connection, String nomeTabela) throws SQLException
    {
        String pkColumn = obterNomeColunaPK(connection, nomeTabela);
        String seq = consultarSequenciasPorTabela(connection, nomeTabela);

        if(seq == null)   return "";
    
        String query = String.format(
            "SELECT setval('%s', " +
            "COALESCE((SELECT MAX(CASE WHEN %s::TEXT ~ '^[0-9]+$' THEN %s::BIGINT ELSE NULL END) FROM %s), 1) +1, false);",
            seq, pkColumn, pkColumn, nomeTabela);

        return query;
    }
    
    
    public  String consultarSequenciasPorTabela(Connection connection, String nomeTabela)
    {
        String seq = null;

        String query = "SELECT s.sequencename " +
                       "FROM pg_sequences s " +
                       "JOIN pg_class c ON c.relname = s.sequencename " +
                       "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                       "AND s.sequencename LIKE '" + nomeTabela + "_%_seq';";

        try (java.sql.Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query))
        {

            while (resultSet.next())
            {
                String sequenceName = resultSet.getString("sequencename");
      
                seq = sequenceName;
                
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        
        return seq != null ?  seq : null;
    }

}
