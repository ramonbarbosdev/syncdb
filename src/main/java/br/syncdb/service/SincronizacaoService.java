package br.syncdb.service;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import java.util.concurrent.TimeoutException;
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

    @Autowired
    private EstruturaService estruturaService;
     
    public Map<String, Object> executarSincronizacao(String base, String nomeTabela )
    {
        Map<String, Object> response = new HashMap<>();
        try
        {

            Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            Connection conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            CompletableFuture<Set<String>> futureLocal = CompletableFuture.supplyAsync(() -> 
                    databaseService.obterTabelaMetaData(base, conexaoLocal)
                );

            Set<String> nomeTabelaLocal = futureLocal.get(5, TimeUnit.MINUTES);

            CompletableFuture<Set<String>> futureCloud = CompletableFuture.supplyAsync(() -> 
            databaseService.obterTabelaMetaData(base, conexaoCloud));

            Set<String> nomeTabelaCloud = futureCloud.get(5, TimeUnit.MINUTES);

            if(nomeTabela == null)
            {
                estruturaService.processarTabelas(conexaoCloud, conexaoLocal, nomeTabelaCloud, nomeTabelaLocal, response,base, null);
            }
            else
            {
                estruturaService.processarTabelas(conexaoCloud, conexaoLocal, nomeTabelaCloud, nomeTabelaLocal, response,base, nomeTabela);
            }
           
        }
        catch (SQLException e)
        {
            response.put("success", false);
            response.put("errorType", "DATABASE_ERROR");
            response.put("message", "Erro de SQL durante sincronização: " + e.getMessage());
        
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            response.put("success", false);
            response.put("errorType", "INTERRUPTED");
            response.put("message", "Execução interrompida");
            
        }
        catch (TimeoutException e)
        {
            response.put("success", false);
            response.put("errorType", "TIMEOUT");
            response.put("message", "Tempo limite excedido na operação");
            
        }
        catch (Exception e)
        {
            response.put("success", false);
            response.put("errorType", "UNEXPECTED_ERROR");
            response.put("message", "Erro inesperado durante sincronização");
            response.put("details", e.getClass().getSimpleName() + ": " + e.getMessage());
            
        } finally
        {
            ConexaoBanco.fecharTodos();
        }

        return response;
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
