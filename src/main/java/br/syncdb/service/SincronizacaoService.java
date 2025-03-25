package br.syncdb.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import br.syncdb.config.ConexaoBanco;
import br.syncdb.controller.TipoConexao;
import br.syncdb.model.Coluna;

@Service
public class SincronizacaoService
{
    @Autowired
    private DatabaseService databaseService;

    //todo: implementar a o insert no momento que criar os scripts
    
    public void executarSincronizacaoTotal(String base, String banco)
    {
        try {
            Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            Connection conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);
           
            CompletableFuture<Set<String>> futureCloud = CompletableFuture.supplyAsync(() -> 
                databaseService.obterTabelaMetaData(base, conexaoCloud));

            CompletableFuture<Set<String>> futureLocal = CompletableFuture.supplyAsync(() -> 
                databaseService.obterTabelaMetaData(base, conexaoLocal));
                
                // Esperar com timeout de 5 minutos
            Set<String> nomeTabelaCloud = futureCloud.get(5, TimeUnit.MINUTES);

            Set<String> nomeTabelaLocal = futureLocal.get(5, TimeUnit.MINUTES);
                
            processarTabelas(conexaoCloud, conexaoLocal, nomeTabelaCloud, nomeTabelaLocal);
            
        } catch (SQLException e) {
            logError("Erro de SQL durante sincronização", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logError("Execução interrompida", e);
        } catch (Exception e) {
            logError("Erro inesperado durante sincronização", e);
        }
    }

    private void processarTabelas(Connection conexaoCloud, Connection conexaoLocal, 
        Set<String> nomeTabelaCloud, Set<String> nomeTabelaLocal) throws SQLException
    {
        // estruturas thread-safe
        List<String> sequencias = Collections.synchronizedList(new ArrayList<>());
        List<String> funcoes = Collections.synchronizedList(new ArrayList<>());
        List<String> criacoesTabela = Collections.synchronizedList(new ArrayList<>());
        List<String> chavesEstrangeiras = Collections.synchronizedList(new ArrayList<>());
        List<String> alteracoes = Collections.synchronizedList(new ArrayList<>());

        // Processar sequências uma única vez
        String sequenciaQuery = databaseService.criarSequenciaQuery(conexaoCloud, conexaoLocal);
        if (sequenciaQuery != null)
        {
            sequencias.add(sequenciaQuery);
        }

        // Thread Pool para processamento paralelo
        int threadCount = Math.min(Runtime.getRuntime().availableProcessors() * 2, 16);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        
        try 
        {
           
            List<CompletableFuture<Void>> futures = nomeTabelaCloud.stream()
                .map(nomeTabela -> CompletableFuture.runAsync(() -> 
                    processarTabelaIndividual(
                        conexaoCloud, conexaoLocal, nomeTabela, nomeTabelaLocal,
                        criacoesTabela, chavesEstrangeiras, alteracoes), executor))
                .collect(Collectors.toList());

  
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Processamento interrompido", e);
        } catch (ExecutionException e) {
            throw new SQLException("Erro durante processamento paralelo", e);
        } finally {
            executor.shutdownNow();
        }

        executarQueriesEmLotes(conexaoLocal, sequencias, funcoes, criacoesTabela, chavesEstrangeiras, alteracoes);
    }

    private void processarTabelaIndividual(Connection conexaoCloud, Connection conexaoLocal,
        String nomeTabela, Set<String> nomeTabelaLocal,
        List<String> criacoesTabela, List<String> chavesEstrangeiras,
        List<String> alteracoes)
    {
        
        try
        {
            if (!nomeTabelaLocal.contains(nomeTabela))
            {
                // Processar nova tabela
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

            }
            else
            {
                // Processar alterações
                String alterQuery = databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, nomeTabela);

                if (alterQuery != null)
                {
                    alteracoes.add(alterQuery);
                }
            }

        }
        catch (SQLException e)
        {
            logError("Erro ao processar tabela " + nomeTabela, e);
        }
    }

    private void executarQueriesEmLotes(Connection conexaoLocal, 
        List<String> sequencias, List<String> funcoes, 
        List<String> criacoesTabela, List<String> chavesEstrangeiras,
        List<String> alteracoes) throws SQLException
    {
        
        conexaoLocal.setAutoCommit(false);
        
        try
        {
            executarLoteComThreadPool(conexaoLocal, sequencias, "Sequências");
            executarLoteComThreadPool(conexaoLocal, criacoesTabela, "Criação de Tabelas");
            executarLoteComThreadPool(conexaoLocal, alteracoes, "Alterações");
            executarLoteComThreadPool(conexaoLocal, chavesEstrangeiras, "Chaves Estrangeiras");
            executarLoteComThreadPool(conexaoLocal, funcoes, "Funções");
            
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
        
        try {
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
                        
                        // Log de progresso a cada 100 queries
                        if (successCount.get() % 100 == 0)
                        {
                            System.out.printf("[%s] Progresso: %d/%d%n", 
                                            tipo, successCount.get(), queries.size());
                        }

                    }
                    catch (SQLException e)
                    {
                        errorCount.incrementAndGet();
                        logError(String.format("[%s] Erro na query", tipo), e);
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

            System.out.printf("[%s] Concluído em %d ms. Sucessos: %d, Erros: %d%n",
                            tipo, duration, successCount.get(), errorCount.get());

            if (errorCount.get() > 0)
            {
                throw new SQLException(String.format("%d erros durante execução de %s", 
                                                errorCount.get(), tipo));
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

    private void logError(String message, Exception e)
    {
        System.err.println(message + ": " + e.getMessage());
        e.printStackTrace(System.err);
    }

    // private void executarLotes(Statement stmt, List<String> queries, String tipo) throws SQLException {
    //     if (queries.isEmpty()) {
    //         System.out.printf("[%s] Nenhuma query para executar.%n", tipo);
    //         return;
    //     }
    
    //     final int batchSize = 50; // Reduzi o tamanho do batch para evitar problemas
    //     final int totalQueries = queries.size();
    //     int count = 0;
    //     long startTime = System.currentTimeMillis();
    
    //     System.out.printf("[%s] Iniciando execução de %d queries...%n", tipo, totalQueries);
    
    //     for (String query : queries) {
    //         try {
    //             stmt.addBatch(query);
    //             count++;
    
    //             if (count % batchSize == 0) {
    //                 int[] updateCounts = stmt.executeBatch();
    //                 verificarResultadosBatch(updateCounts, tipo);
    //                 printProgress(tipo, count, totalQueries, startTime);
    //             }
    //         } catch (SQLException e) {
    //             System.err.printf("Erro na query %d: %s%nQuery: %s%n", count, e.getMessage(), query);
    //             throw e;
    //         }
    //     }
    
    //     // Executar o restante
    //     if (count % batchSize != 0) {
    //         int[] updateCounts = stmt.executeBatch();
    //         verificarResultadosBatch(updateCounts, tipo);
    //         printProgress(tipo, count, totalQueries, startTime);
    //     }
    
    //     long endTime = System.currentTimeMillis();
    //     System.out.printf("[%s] Concluído! %d queries executadas em %.2f segundos.%n", 
    //                      tipo, totalQueries, (endTime - startTime) / 1000.0);
    // }
    
    // private void verificarResultadosBatch(int[] updateCounts, String tipo) {
    //     for (int i = 0; i < updateCounts.length; i++) {
    //         if (updateCounts[i] == Statement.SUCCESS_NO_INFO) {
    //             System.out.printf("[%s] Aviso: Sucesso sem informação no comando %d%n", tipo, i+1);
    //         } else if (updateCounts[i] == Statement.EXECUTE_FAILED) {
    //             System.out.printf("[%s] Erro: Falha na execução do comando %d%n", tipo, i+1);
    //         }
    //         // Valores positivos indicam número de linhas afetadas (OK)
    //     }
    // }
    // private void printProgress(String tipo, int processed, int total, long startTime) {
    //     double percentage = (processed * 100.0) / total;
    //     long currentTime = System.currentTimeMillis();
    //     double elapsedSec = (currentTime - startTime) / 1000.0;
        
    //     // Estimativa de tempo restante
    //     double estimatedTotalSec = elapsedSec * total / processed;
    //     double remainingSec = estimatedTotalSec - elapsedSec;
        
    //     System.out.printf("[%s] Progresso: %d/%d (%.2f%%) | Tempo: %.2fs | Restante: ~%.2fs%n",
    //                      tipo, processed, total, percentage, elapsedSec, remainingSec);
    // }
    
 
    
    // 🏆 Método para obter a estrutura de colunas
    
    

   
}
