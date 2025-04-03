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

import br.syncdb.model.EstruturaTabela;

@Service
public class EstruturaService {
    
    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private QueryArquivoService queryArquivoService;
     
    
    public void processarTabelas(Connection conexaoCloud,
    Connection conexaoLocal,
    Set<String> tabelasCloud,
    Set<String> nomeTabelaLocal,
    List<EstruturaTabela> detalhes,
    String base,
    String nomeTabelaUni
    ) 
    throws SQLException
    {
        //TO:DO - FAZER RETORNO 
        //TO:DO - ACHAR A ORIGEM DO ERRO
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
        }
    
        for (String itemTabela : tabelasCloud)
        {
            EstruturaTabela infoEstrutura = new EstruturaTabela();
            
            if (!nomeTabelaLocal.contains(itemTabela))
            {
                System.out.println("Criando estrutura da tabela: " + itemTabela);
                
                String createTable = databaseService.criarEstuturaTabela(conexaoCloud, itemTabela);
                if (createTable != null)
                {
                    criacoesTabela.add(createTable);
                    infoEstrutura.setTabela(itemTabela);
                    infoEstrutura.setAcao("create");
                    infoEstrutura.setQuerys(createTable.length());
                    detalhes.add(infoEstrutura);
                }
                
                String fkQuery = databaseService.obterChaveEstrangeira(conexaoCloud, itemTabela);
                if (fkQuery != null)
                {
                    chavesEstrangeiras.add(fkQuery);
                }

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
                    infoEstrutura.setQuerys(alterQuery.length());
                    detalhes.add(infoEstrutura);
                }
               
            }
        }
        

        HashMap<String, List<String>> queries = new LinkedHashMap<>();
        queries.put("Sequências", sequencias);
        queries.put("Criação de Tabelas", criacoesTabela);
        queries.put("Chaves Estrangeiras",chavesEstrangeiras);
        queries.put("Alterações",alteracoes);
        // queries.put("Criação de Tabelas", funcoes);
        
        executarQueriesEmLotes(conexaoLocal, queries);
        
        // queryArquivoService.salvarQueriesAgrupadas(diretorio, queries);
        // response.put("pastaQueries", diretorio.getAbsolutePath());
    }

    private void executarQueriesEmLotes(Connection conexaoLocal, HashMap<String, List<String>> queries) throws SQLException
    {
        
        conexaoLocal.setAutoCommit(false);
        
        try
        {
            for (String i : queries.keySet())
            {
                executarLoteComThreadPool(conexaoLocal, queries.get(i), i);
            }
           
            conexaoLocal.commit();
        }
        catch (SQLException e)
        {
            conexaoLocal.rollback();
            throw e;
        }
    }

    private void executarLoteComThreadPool(Connection conexao, List<String> queries, String tipo) 
        throws SQLException
    {
        

        if (queries.isEmpty())
        {
            System.out.printf("[%s] Nenhuma query para executar.%n", tipo);
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
                        System.err.println("["+tipo+"] Erro na query: "+query+"" );

                    }
                    finally
                    {
                        latch.countDown();
                    }

                });
            }

            // Aguardar conclusão
            latch.await();
 
            if (errorCount.get() > 0)
            {
                throw new SQLException(String.format("%d erros durante execução de %s", errorCount.get(), tipo));
            }
            
            
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
