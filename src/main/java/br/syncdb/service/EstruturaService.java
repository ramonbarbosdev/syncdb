package br.syncdb.service;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EstruturaService {
    
    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private QueryArquivoService queryArquivoService;
     
    
    public void processarTabelas(Connection conexaoCloud,
    Connection conexaoLocal,
    Set<String> nomeTabelaCloud,
    Set<String> nomeTabelaLocal,
    Map<String, Object> response,
    String base,
    String nomeTabelaUni
    ) 
    throws SQLException
    {
        
        String pastaQueries = "src\\main\\java\\br\\syncdb\\query\\queries_"+base;
        File diretorio = new File(pastaQueries);
        
        if (!diretorio.exists())
        {
            diretorio.mkdir();
        }

        List<String> sequencias = Collections.synchronizedList(new ArrayList<>());
        List<String> funcoes = Collections.synchronizedList(new ArrayList<>());
        List<String> criacoesTabela = Collections.synchronizedList(new ArrayList<>());
        List<String> chavesEstrangeiras = Collections.synchronizedList(new ArrayList<>());
        List<String> alteracoes = Collections.synchronizedList(new ArrayList<>());

        String sequenciaQuery = databaseService.criarSequenciaQuery(conexaoCloud, conexaoLocal);
        
        if (sequenciaQuery != null)
        {
            sequencias.add(sequenciaQuery);
            response.put("sequencia", "SEQUENCIA_CRIADA");
        }

        int threadCount = Math.min(Runtime.getRuntime().availableProcessors() * 2, 16);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        try 
        {
            
            List<CompletableFuture<Void>> futures = nomeTabelaCloud.stream()
            .map(tabela -> CompletableFuture.runAsync(() ->
            {
                if (nomeTabelaUni != null && !tabela.equals( nomeTabelaUni))
                {
                    return; 
                }
        
                 processarTabelaIndividual(conexaoCloud, conexaoLocal, tabela,  nomeTabelaLocal, criacoesTabela, chavesEstrangeiras, alteracoes ,   response);
              

            }, executor))
            .collect(Collectors.toList());
  
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new SQLException("Processamento interrompido", e);
        }
        catch (ExecutionException e)
        {
            throw new SQLException("Erro durante processamento paralelo", e);
        } 
        finally
        {
            executor.shutdownNow();
        }

        HashMap<String, List<String>> queries = new LinkedHashMap<>();
        queries.put("Sequências", sequencias);
        queries.put("Criação de Tabelas", criacoesTabela);
        queries.put("Chaves Estrangeiras",chavesEstrangeiras);
        queries.put("Alterações",alteracoes);
        // queries.put("Criação de Tabelas", funcoes);
        
        executarQueriesEmLotes(conexaoLocal, queries, response);
        
        // queryArquivoService.salvarQueriesAgrupadas(diretorio, queries);
        // response.put("pastaQueries", diretorio.getAbsolutePath());
    }

    private void processarTabelaIndividual(Connection conexaoCloud, Connection conexaoLocal,
        String nomeTabela,
        Set<String> nomeTabelaLocal,
        List<String> criacoesTabela,
        List<String> chavesEstrangeiras,
        List<String> alteracoes,
        Map<String, Object> response)
    {
        try
        {
            if (!nomeTabelaLocal.contains(nomeTabela))
            {
                
                String createTable = databaseService.criarEstuturaTabela(conexaoCloud, nomeTabela);

                if (createTable != null)
                {       
                    criacoesTabela.add(createTable);
                }
                
                String fkQuery = databaseService.obterChaveEstrangeira(conexaoCloud, nomeTabela);

                if (fkQuery != null)
                {
                    chavesEstrangeiras.add(fkQuery);
                }
                response.put(nomeTabela + "_status", "CRIACAO_CONCLUIDA");

            }
            else
            {
                String alterQuery = databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, nomeTabela);

                if (alterQuery != null)
                {                  
                    alteracoes.add(alterQuery);
                    response.put(nomeTabela + "_status", "ALTERACAO_CONCLUIDA");
                }
            }

        }
        catch (SQLException e)
        {
            logRetorno("Erro ao processar tabela " + nomeTabela, e);
        }

  
    }




    private void executarQueriesEmLotes(Connection conexaoLocal, HashMap<String, List<String>> queries, Map<String, Object> response) throws SQLException
    {
        
        conexaoLocal.setAutoCommit(false);
        
        try
        {
            for (String i : queries.keySet())
            {
                executarLoteComThreadPool(conexaoLocal, queries.get(i), i, response);
            }
           
            conexaoLocal.commit();
        }
        catch (SQLException e)
        {
            conexaoLocal.rollback();
            throw e;
        }
    }

    private void executarLoteComThreadPool(Connection conexao, List<String> queries, String tipo, Map<String, Object> response) 
        throws SQLException
    {
        
        response.put("success", true);
        
        if (queries.isEmpty())
        {
            System.out.printf("[%s] Nenhuma query para executar.%n", tipo);
            response.put("["+tipo+"]", "Nenhuma query para executar.");
            return;
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
            Math.min(queries.size(), Runtime.getRuntime().availableProcessors() * 2));
        
        try
        {
            // Contador progresso
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(queries.size());

            System.out.printf("[%s] Iniciando execução de %d queries%n", tipo, queries.size());
            long startTime = System.currentTimeMillis();

            for (String query : queries)
            {
                executor.execute(() -> {

                    try (Statement stmt = conexao.createStatement())
                    {
                        stmt.execute(query);
                        successCount.incrementAndGet();
                        
                        if (successCount.get() % 100 == 0)
                        {
                            System.out.printf("[%s] Progresso: %d/%d%n", 
                                            tipo, successCount.get(), queries.size());
                        }

                    }
                    catch (SQLException e)
                    {
                        
                        errorCount.incrementAndGet();
                        logRetorno(String.format("[%s] Erro na query", tipo), e);

                        response.put("success", false);
                        response.put("message", "Erro na query");
                        response.put("details", e.getMessage());

                    }
                    finally
                    {
                        latch.countDown();
                    }

                });
            }

            // Aguardar conclusão
            latch.await();
            
            // Log final
            long duration = System.currentTimeMillis() - startTime;

            // response.put("message", "Concluído em "+duration+" ms. Sucessos: "+successCount.get()+", Erros: "+errorCount.get()+"");

            if (errorCount.get() > 0)
            {
                throw new SQLException(String.format("%d erros durante execução de %s", errorCount.get(), tipo));
            }
            
            response.put("message", "Tabelas Sincronizadas.");
            
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new SQLException("Execução interrompida", e);

        }
        finally
        {
            executor.shutdownNow();
        }
    }

    private String logRetorno(String message, Exception e)
    {
        String msgRetorno = message;

        if(e != null)
        {
            System.err.println(message + ": " + e.getMessage());
            // e.printStackTrace(System.err);

            msgRetorno = message + ": " + e.getMessage();
        }

        return msgRetorno;
       
    }

    public  Set<String> obterTabelas(Connection conexao, String base) throws InterruptedException, ExecutionException, TimeoutException
    {
        CompletableFuture<Set<String>> futureCloud = CompletableFuture.supplyAsync
        (() -> 
            databaseService.obterTabelaMetaData(base, conexao)
        );

        Set<String> tabelas = futureCloud.get(5, TimeUnit.MINUTES);

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
