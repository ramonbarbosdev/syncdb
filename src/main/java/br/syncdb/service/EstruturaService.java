package br.syncdb.service;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import br.syncdb.model.EstruturaTabela;

@Service
public class EstruturaService {
    
    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private QueryArquivoService queryArquivoService;

    @Autowired
    private ProcessoService processoService;
     
    public HashMap<String, List<String>>  processarTabelas(Connection conexaoCloud,
    Connection conexaoLocal,
    Set<String> tabelasCloud,
    Set<String> nomeTabelaLocal,
    List<EstruturaTabela> detalhes,
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

        int totalTabelas = tabelasCloud.size();
        AtomicInteger tabelasProcessadas = new AtomicInteger(0);

        processoService.enviarProgresso("Iniciando", 0, "Iniciando processam de " + totalTabelas + " tabelas", null);
        
        if (sequenciaQuery != null)
        {
            sequencias.add(sequenciaQuery);
        }
    
        for (String itemTabela : tabelasCloud)
        {
            int progresso = (int) ((tabelasProcessadas.incrementAndGet() / (double) totalTabelas) * 100);
            processoService.enviarProgresso("Processando", progresso, "Processando tabela: " + itemTabela, itemTabela);

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
                    // infoEstrutura.setQuerys(createTable.length());
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
                    // infoEstrutura.setQuerys(alterQuery.length());
                    detalhes.add(infoEstrutura);
                }
               
            }
        }

        processoService.enviarProgresso("Concluido", 100, "Processamento concluído com sucesso", null);
        

        HashMap<String, List<String>> queries = new LinkedHashMap<>();
        queries.put("Sequências", sequencias);
        queries.put("Criação de Tabelas", criacoesTabela);
        queries.put("Chaves Estrangeiras",chavesEstrangeiras);
        queries.put("Alterações",alteracoes);
        // queries.put("Criação de Tabelas", funcoes);
        return queries;
        // queryArquivoService.salvarQueriesAgrupadas(diretorio, queries);
        // response.put("pastaQueries", diretorio.getAbsolutePath());
    }

    public void executarQueriesEmLotes(Connection conexao, Map<String, List<String>> queries, List<EstruturaTabela> detalhes) {
        for (Map.Entry<String, List<String>> entry : queries.entrySet())
        {
            String tipo = entry.getKey();
            List<String> lista = entry.getValue();
          
            
            System.out.println("\n=== Executando grupo de queries: " + tipo + " ===");
    
            for (String query : lista)
            {
                String tabela = extrairNomeTabelaDaQuery(query);

                try (Statement stmt = conexao.createStatement())
                {
                    stmt.execute(query);
                    System.out.println("Executada com sucesso:\n" + query);
                }
                catch (SQLException e) 
                {
                    System.err.println("Erro ao executar query:\n" + query);
                    System.err.println("Mensagem do erro: " + e.getMessage()+ "\n");

                    for (EstruturaTabela detalhe : detalhes)
                    {
                        if (detalhe.getTabela().equalsIgnoreCase(tabela))
                        {
                            detalhe.setErro(e.getMessage()); // Supondo que você tenha um campo 'erro' em EstruturaTabela
                            break;
                        }
                    }
                }
            }
        }
    }

    public String extrairNomeTabelaDaQuery(String query) {
        query = query.trim().toUpperCase();
    
        String patternCreate = "CREATE TABLE IF NOT EXISTS ([\\w\\.]+)";
        String patternCreateSimple = "CREATE TABLE ([\\w\\.]+)";
        String patternAlter = "ALTER TABLE ([\\w\\.]+)";
        String patternForeign = "ALTER TABLE ONLY ([\\w\\.]+)";
        String patternInsert = "INSERT INTO ([\\w\\.]+)";
        
        List<String> patterns = Arrays.asList(patternCreate, patternCreateSimple, patternAlter, patternForeign, patternInsert);
    
        for (String patternStr : patterns)
        {
            Pattern pattern = Pattern.compile(patternStr);
            Matcher matcher = pattern.matcher(query);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
    
        return "desconhecida"; // Caso não consiga identificar
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
