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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import org.hibernate.boot.model.relational.Database;
import org.jooq.exception.DataAccessException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.github.benmanes.caffeine.cache.Cache;

import br.syncdb.config.ConexaoBanco;
import br.syncdb.controller.TipoConexao;
import br.syncdb.model.Coluna;
import br.syncdb.model.EstruturaTabela;
import br.syncdb.model.TabelaDetalhe;

@Service
public class SincronizacaoService
{
    @Autowired
    private EstruturaService estruturaService;

    @Autowired
    private DadosService dadosService;
    
    @Autowired
    private OperacaoBancoService operacaoBancoService;

    @Autowired
    private CacheService cacheService;

    @Autowired
    private DatabaseService databaseService;

    private final Semaphore estruturaLock = new Semaphore(1);

    // @Scheduled(cron = "* */1 * * * *") 
    public void executarVerificacaoAgendada()
    {
        verificarAsync();
    }
    
    public void verificarAsync()
    {
        List<String> basesCloud = databaseService.listarBases("w5i_tecnologia", TipoConexao.CLOUD);
        List<String> basesLocal = databaseService.listarBases("syncdb", TipoConexao.LOCAL);

        List<String> basesVerificacao = new ArrayList<>();

        for (String baseLocal : basesLocal) {
            List<String> coincidentes = basesCloud.stream()
                                        .filter(t -> t.equals(baseLocal))
                                        .collect(Collectors.toList());
            basesVerificacao.addAll(coincidentes);
        }

        List<String> basesParaAtualizar = new ArrayList<>();

        for (String database : basesVerificacao) 
        {
            List<String> esquemasCloud = databaseService.obterSchema(database, null, TipoConexao.CLOUD);

            for (String esquemaCloud : esquemasCloud) 
            {
                try
                {
                    estruturaLock.acquire();
                    Map<String, Object> estruturaTabelaCloud = estruturaService.verificarEstrutura(database, esquemaCloud, null);

                    System.out.println("Verificando estrutura");
                    if (estruturaTabelaCloud != null)
                    {
                        if (estruturaTabelaCloud.containsKey("tabelas_afetadas"))
                        {
                            List<String> tabelasAfetadas = (List<String>) estruturaTabelaCloud.get("tabelas_afetadas");
    
                            if (tabelasAfetadas != null && !tabelasAfetadas.isEmpty())
                            {
                                basesParaAtualizar.add(database);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Thread.currentThread().interrupt();
                    System.err.println("Execução interrompida.");
                }
                finally
                {
                    estruturaLock.release(); 
                }
            }
        }

      System.out.println("Bases que devem ser atualizadas: " + basesParaAtualizar); 
    }
   
}
