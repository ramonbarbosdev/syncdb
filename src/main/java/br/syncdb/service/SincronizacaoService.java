package br.syncdb.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import java.util.concurrent.CompletableFuture;
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


    public void executarCriacaoTabela(String base, String banco) {
        try {
            // Usar try-with-resources para garantir fechamento das conexões
            try (Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
                Connection conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL)) {
                
                // Obter metadados em paralelo
                CompletableFuture<Set<String>> futureCloud = CompletableFuture.supplyAsync(() -> 
                    databaseService.obterTabelaMetaData(base, conexaoCloud));
                CompletableFuture<Set<String>> futureLocal = CompletableFuture.supplyAsync(() -> 
                    databaseService.obterTabelaMetaData(base, conexaoLocal));
                
                Set<String> nomeTabelaCloud = futureCloud.join();
                Set<String> nomeTabelaLocal = futureLocal.join();
                
                // Processamento paralelo das tabelas
                processarTabelas(conexaoCloud, conexaoLocal, nomeTabelaCloud, nomeTabelaLocal);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    
    private void processarTabelas(Connection conexaoCloud, Connection conexaoLocal, 
        Set<String> nomeTabelaCloud, Set<String> nomeTabelaLocal) throws SQLException {
        // Usar listas separadas para cada tipo de operação
        List<String> sequencias = new ArrayList<>();
        List<String> funcoes = new ArrayList<>();
        List<String> criacoesTabela = new ArrayList<>();
        List<String> chavesEstrangeiras = new ArrayList<>();
        List<String> alteracoes = new ArrayList<>();

            
        synchronized (sequencias)
        {
            if(databaseService.criarSequenciaQuery(conexaoCloud, conexaoLocal) != null)
            {
                sequencias.add(databaseService.criarSequenciaQuery(conexaoCloud, conexaoLocal));
            }
        }

        synchronized (funcoes)
        {
            // funcoes.add(databaseService.criarFuncoesQuery(conexaoCloud,conexaoLocal));
        }

        // Processamento paralelo das tabelas
        nomeTabelaCloud.parallelStream().forEach(nomeTabela -> {
            try
            {
                if (!nomeTabelaLocal.contains(nomeTabela))
                {
                    synchronized (criacoesTabela) 
                    {
                        criacoesTabela.add(databaseService.obterEstruturaTabela(conexaoCloud, nomeTabela));
                    }
                    synchronized (chavesEstrangeiras)
                    {
                        chavesEstrangeiras.add(databaseService.obterChaveEstrangeira(conexaoCloud, nomeTabela));
                    }
                }
                else
                {
                    synchronized (alteracoes)
                    {
                        if(databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, nomeTabela) != null)
                        {
                            alteracoes.add(databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, nomeTabela));
                        }
                    }
                }
            } catch (SQLException ex)
            {
                ex.printStackTrace();
            }
        });

 
        executarBatch(conexaoLocal, sequencias, funcoes, criacoesTabela, chavesEstrangeiras, alteracoes);
    }

                
    private void executarBatch(Connection conexaoLocal, List<String> sequencias, 
        List<String> funcoes, List<String> criacoesTabela,
        List<String> chavesEstrangeiras, List<String> alteracoes) throws SQLException
        {
        conexaoLocal.setAutoCommit(false);

        try (Statement stmt = conexaoLocal.createStatement())
        {
            // Executar cada tipo de operação em lotes menores
            executarLotes(stmt, sequencias, "Sequências");
            executarLotes(stmt, funcoes, "Funções");
            executarLotes(stmt, criacoesTabela, "Criação de Tabelas");
            executarLotes(stmt, alteracoes, "Alterações");
            executarLotes(stmt, chavesEstrangeiras, "Chaves Estrangeiras");

            conexaoLocal.commit();
        }
        catch (SQLException e)
        {
            conexaoLocal.rollback();
            throw e;
        }
    }

    private void executarLotes(Statement stmt, List<String> queries, String tipo) throws SQLException {
        if (queries.isEmpty()) {
            System.out.printf("[%s] Nenhuma query para executar.%n", tipo);
            return;
        }
    
        final int batchSize = 100; // Tamanho do lote ajustável
        final int totalQueries = queries.size();
        int count = 0;
        long startTime = System.currentTimeMillis();
    
        System.out.printf("[%s] Iniciando execução de %d queries...%n", tipo, totalQueries);
    
        for (String query : queries) {
            stmt.addBatch(query);
            count++;
    
            if (count % batchSize == 0) {
                stmt.executeBatch();
                printProgress(tipo, count, totalQueries, startTime);
            }
        }
    
        // Executar o restante
        if (count % batchSize != 0) {
            stmt.executeBatch();
            printProgress(tipo, count, totalQueries, startTime);
        }
    
        long endTime = System.currentTimeMillis();
        System.out.printf("[%s] Concluído! %d queries executadas em %.2f segundos.%n", 
                         tipo, totalQueries, (endTime - startTime) / 1000.0);
    }
    
    private void printProgress(String tipo, int processed, int total, long startTime) {
        double percentage = (processed * 100.0) / total;
        long currentTime = System.currentTimeMillis();
        double elapsedSec = (currentTime - startTime) / 1000.0;
        
        // Estimativa de tempo restante
        double estimatedTotalSec = elapsedSec * total / processed;
        double remainingSec = estimatedTotalSec - elapsedSec;
        
        System.out.printf("[%s] Progresso: %d/%d (%.2f%%) | Tempo: %.2fs | Restante: ~%.2fs%n",
                         tipo, processed, total, percentage, elapsedSec, remainingSec);
    }
    
 
    
    // 🏆 Método para obter a estrutura de colunas
    
    

   
}
