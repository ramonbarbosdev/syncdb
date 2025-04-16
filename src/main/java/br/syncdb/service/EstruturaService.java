package br.syncdb.service;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.github.benmanes.caffeine.cache.Cache;

import br.syncdb.config.ConexaoBanco;
import br.syncdb.controller.TipoConexao;
import br.syncdb.model.EstruturaTabela;
import br.syncdb.utils.UtilsSync;

@Service
public class EstruturaService {
    
    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private ProcessoService processoService;

    @Autowired
    private UtilsSync utilsSync;

    @Autowired
    private OperacaoBancoService operacaoBancoService;

    @Autowired
    private CacheService cacheService;

    public Map<String, Object> verificarEstrutura(String base, String nomeTabela)
    {
        Map<String, Object> response = new LinkedHashMap<>(); 
        List<EstruturaTabela> detalhes = new ArrayList<>();
        
        Connection conexaoCloud = null;
        Connection conexaoLocal = null; 
        try
        {
            conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            Set<String> tabelasLocal = obterTabelas(conexaoLocal, base, nomeTabela);
            Set<String> tabelasCloud = obterTabelas(conexaoCloud, base, nomeTabela);

            HashMap<String, List<String>> queries = processarTabelas(conexaoCloud, conexaoLocal, tabelasCloud, tabelasLocal, detalhes,base, nomeTabela);

            cacheService.salvarCache(base + ":" + nomeTabela, queries);
            
            response.put("tabelas_afetadas", detalhes); 
            response.put("sucesso", true); 
        }
        catch (Exception e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        }

        return response;
    }

    public Map<String, Object> sincronizarEstrutura(String base)
    {
        Map<String, Object> response = new LinkedHashMap<>();
        List<Map<String, String>> detalhes = new ArrayList<>();

        Connection conexaoLocal = null; 
        try
        {
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);
            @SuppressWarnings("unchecked")
            HashMap<String, List<String>> querys = cacheService.buscarCache(base + ":", HashMap.class);

            if (querys == null)
            {
                response.put("sucesso", false);
                response.put("mensagem", "Nenhuma verificação foi feita previamente.");
                return response;
            }
            
            operacaoBancoService.executarQueriesEmLotes(conexaoLocal, querys, detalhes);

            response.put("sucesso", true); 
            response.put("tabelas_afetadas", detalhes); 
            response.put("mensagem", "Estrutura Sincronizada.");
        
        }
        catch (Exception e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
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


    public HashMap<String, List<String>>  processarTabelas(Connection conexaoCloud,
    Connection conexaoLocal,
    Set<String> tabelasCloud,
    Set<String> nomeTabelaLocal,
    List<EstruturaTabela> detalhes,
    String base,
    String nomeTabela
    ) 
    throws SQLException
    {
        List<String> criacaoSchema = Collections.synchronizedList(new ArrayList<>());
        List<String> sequencias = Collections.synchronizedList(new ArrayList<>());
        List<String> funcoes = Collections.synchronizedList(new ArrayList<>());
        List<String> criacoesTabela = Collections.synchronizedList(new ArrayList<>());
        List<String> chavesEstrangeiras = Collections.synchronizedList(new ArrayList<>());
        List<String> alteracoes = Collections.synchronizedList(new ArrayList<>());

        int totalTabelas = tabelasCloud.size();
        AtomicInteger tabelasProcessadas = new AtomicInteger(0);
        processoService.enviarProgresso("Iniciando", 0, "Iniciando processam de " + totalTabelas + " tabelas", null);
        
        Set<String> schemasCriados = new HashSet<>();
        
        String sequenciaQuery = databaseService.criarSequenciaQuery(conexaoCloud, conexaoLocal);
        if (sequenciaQuery != null)  sequencias.add(sequenciaQuery);

        for (String itemTabela : tabelasCloud)
        {
           
            int progresso = (int) ((tabelasProcessadas.incrementAndGet() / (double) totalTabelas) * 100);
            processoService.enviarProgresso("Processando", progresso, "Processando tabela: " + itemTabela, itemTabela);

            EstruturaTabela infoEstrutura = new EstruturaTabela();
            
            if (!nomeTabelaLocal.contains(itemTabela))
            {
                System.out.println("Criando estrutura da tabela: " + itemTabela);
                
                String schema = utilsSync.extrairSchema(itemTabela); 
                if (schema != null && !schemasCriados.contains(schema))
                {
                    String querySchema = databaseService.gerarQueryCriacaoSchemas(conexaoLocal, schema);
                    if (querySchema != null && !querySchema.isBlank())
                    {
                        criacaoSchema.add(querySchema);
                        schemasCriados.add(schema);
                    }
                }

                String queryTabela = databaseService.criarEstuturaTabela(conexaoCloud, itemTabela);
                if (queryTabela != null && !queryTabela.isBlank())
                {
                    criacoesTabela.add(queryTabela);
                    infoEstrutura.setTabela(itemTabela);
                    infoEstrutura.setAcao("create");
                    detalhes.add(infoEstrutura);
                }
                
                String fkQuery = databaseService.obterChaveEstrangeira(conexaoCloud, itemTabela);
                if (fkQuery != null)  chavesEstrangeiras.add(fkQuery);
            }
            else
            {
                System.out.println("Verificando alteracao na tabela: " + itemTabela);

                String alterQuery = databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, itemTabela);
             
                if (alterQuery != null)
                {
                    alteracoes.add(alterQuery);
                    infoEstrutura.setTabela(itemTabela);
                    infoEstrutura.setAcao("update");
                    // infoEstrutura.setQuerys(alterQuery.length());
                    detalhes.add(infoEstrutura);
                }
               
            }
        }

        processoService.enviarProgresso("Concluido", 100, "Processamento concluído com sucesso", null);

        HashMap<String, List<String>> queries = new LinkedHashMap<>();
        queries.put("Schemas", criacaoSchema);
        queries.put("Sequências", sequencias);
        queries.put("Criação de Tabelas", criacoesTabela);
        queries.put("Chaves Estrangeiras",chavesEstrangeiras);
        queries.put("Alterações",alteracoes);
        
        return queries;
    }

    public  Set<String> obterTabelas(Connection conexao, String base, String nomeTabela) throws InterruptedException, ExecutionException, TimeoutException
    {
        CompletableFuture<Set<String>> futureCloud = CompletableFuture.supplyAsync
        (() -> 
            databaseService.obterTabelaMetaData(base, conexao)
        );

        Set<String> tabelas = futureCloud.get(5, TimeUnit.MINUTES);

        if (nomeTabela != null && !nomeTabela.isBlank()) 
        {
            return tabelas.stream()
                    .filter(t -> t.equalsIgnoreCase(nomeTabela))
                    .collect(Collectors.toSet());
        }
    

        return tabelas;
    }

    public void validarEstruturaTabela(Connection conexaoCloud, Connection conexaoLocal, 
    String tabela) throws SQLException
    {
        if (tabela != null && databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, tabela) != null)
        {
            throw new SQLException("Estrutura da tabela " + tabela + " divergente entre cloud e local");
        }
    }

}
