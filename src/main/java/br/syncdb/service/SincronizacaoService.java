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
            sequencias.add(databaseService.criarSequenciaQuery(conexaoCloud, conexaoLocal));
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
                
                synchronized (criacoesTabela) {
                    criacoesTabela.add(databaseService.obterEstruturaTabela(conexaoCloud, nomeTabela));
                }
                synchronized (chavesEstrangeiras) {
                    chavesEstrangeiras.add(databaseService.obterChaveEstrangeira(conexaoCloud, nomeTabela));
                }
            }
            else
            {
                
                synchronized (alteracoes) {
                    alteracoes.add(compararEstruturaTabela(conexaoCloud, conexaoLocal, nomeTabela));
                }
            }
        } catch (SQLException ex) {
        ex.printStackTrace();
        }
        });

        // Executar em batch
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
    
    private String compararEstruturaTabela(Connection conexaoCloud, Connection conexaoLocal, String nomeTabela) throws SQLException {
        StringBuilder alteracoes = new StringBuilder();
    
        DatabaseMetaData metaDataCloud = conexaoCloud.getMetaData();
        DatabaseMetaData metaDataLocal = conexaoLocal.getMetaData();
    
        // ✅ Obter colunas da tabela na origem (Cloud)
        ResultSet colunasCloud = metaDataCloud.getColumns(null, "public", nomeTabela, null);
        Map<String, Coluna> estruturaCloud = new HashMap<>();
    
        while (colunasCloud.next()) {
            Coluna coluna = new Coluna();
            coluna.setNome(colunasCloud.getString("COLUMN_NAME"));
            coluna.setTipo(colunasCloud.getString("TYPE_NAME"));
            coluna.setNullable(colunasCloud.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
            coluna.setDefaultValor(colunasCloud.getString("COLUMN_DEF"));
    
            estruturaCloud.put(coluna.getNome(), coluna);
        }
    
        colunasCloud.close();
    
        // ✅ Obter colunas da tabela no destino (Local)
        ResultSet colunasLocal = metaDataLocal.getColumns(null, "public", nomeTabela, null);
        Map<String, Coluna> estruturaLocal = new HashMap<>();
    
        while (colunasLocal.next()) {
            Coluna coluna = new Coluna();
            coluna.setNome(colunasLocal.getString("COLUMN_NAME"));
            coluna.setTipo(colunasLocal.getString("TYPE_NAME"));
            coluna.setNullable(colunasLocal.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
            coluna.setDefaultValor(colunasLocal.getString("COLUMN_DEF"));
    
            estruturaLocal.put(coluna.getNome(), coluna);
        }
    
        colunasLocal.close();
    
        // ✅ Adicionar novas colunas ou alterar colunas existentes
        for (String nomeColuna : estruturaCloud.keySet()) {
            Coluna colunaCloud = estruturaCloud.get(nomeColuna);
            Coluna colunaLocal = estruturaLocal.get(nomeColuna);
    
            if (colunaLocal == null) {
                // 🔥 Nova coluna → Adicionar ao destino
                alteracoes.append("ALTER TABLE ")
                          .append(nomeTabela)
                          .append(" ADD COLUMN ")
                          .append(colunaCloud.getNome()).append(" ")
                          .append(colunaCloud.getTipo());
    
                if (!colunaCloud.isNullable()) {
                    alteracoes.append(" NOT NULL");
                }
    
                if (colunaCloud.getDefaultValor() != null) {
                    alteracoes.append(" DEFAULT ").append(colunaCloud.getDefaultValor());
                }
    
                alteracoes.append(";\n");
            } else {
                // 🚀 Coluna existe → Verificar diferenças
                if (!colunaCloud.getTipo().equalsIgnoreCase(colunaLocal.getTipo())) {
                    alteracoes.append("ALTER TABLE ")
                              .append(nomeTabela)
                              .append(" ALTER COLUMN ")
                              .append(nomeColuna)
                              .append(" TYPE ")
                              .append(colunaCloud.getTipo())
                              .append(";\n");
                }
    
                if (colunaCloud.isNullable() != colunaLocal.isNullable()) {
                    alteracoes.append("ALTER TABLE ")
                              .append(nomeTabela)
                              .append(" ALTER COLUMN ")
                              .append(nomeColuna)
                              .append(colunaCloud.isNullable() ? " DROP NOT NULL" : " SET NOT NULL")
                              .append(";\n");
                }
    
                if ((colunaCloud.getDefaultValor() == null && colunaLocal.getDefaultValor() != null) ||
                    (colunaCloud.getDefaultValor() != null && !colunaCloud.getDefaultValor().equals(colunaLocal.getDefaultValor()))) {
                    if (colunaCloud.getDefaultValor() == null) {
                        alteracoes.append("ALTER TABLE ")
                                  .append(nomeTabela)
                                  .append(" ALTER COLUMN ")
                                  .append(nomeColuna)
                                  .append(" DROP DEFAULT")
                                  .append(";\n");
                    } else {
                        alteracoes.append("ALTER TABLE ")
                                  .append(nomeTabela)
                                  .append(" ALTER COLUMN ")
                                  .append(nomeColuna)
                                  .append(" SET DEFAULT ")
                                  .append(colunaCloud.getDefaultValor())
                                  .append(";\n");
                    }
                }
            }
        }
    
        // ✅ Remover colunas que não existem mais
        for (String nomeColuna : estruturaLocal.keySet()) {
            if (!estruturaCloud.containsKey(nomeColuna)) {
                alteracoes.append("ALTER TABLE ")
                          .append(nomeTabela)
                          .append(" DROP COLUMN ")
                          .append(nomeColuna)
                          .append(";\n");
            }
        }
    
        return alteracoes.toString();
    }
    
    
    // 🏆 Método para obter a estrutura de colunas
    
    

   
}
