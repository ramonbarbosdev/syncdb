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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import org.jooq.exception.DataAccessException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
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
    private DatabaseService databaseService;

    @Autowired
    private EstruturaService estruturaService;

    @Autowired
    private DadosService dadosService;
    
    @Autowired
    private OperacaoBancoService operacaoBancoService;

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

            HashMap<String, List<String>> querys = dadosService.obterDadosTabela(conexaoCloud,conexaoLocal, tabela, detalhes );

            cacheService.salvarCache(base + ":" + tabela, querys);

            response.put("success", true); 
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

            dadosService.desativarConstraints(conexaoLocal);

            @SuppressWarnings("unchecked")
            HashMap<String, List<String>> querys = cacheService.buscarCache(base + ":" + tabela, HashMap.class);

            if (querys == null)
            {
                response.put("success", false);
                response.put("mensagem", "Nenhuma verificação foi feita previamente.");
                return response;
            }
            
            operacaoBancoService.executarQueriesEmLotes(conexaoLocal, querys, detalhes);
            
            response.put("success", true); 
            response.put("message", "Sincronização de dados concluida"); 
            response.put("tabelas_afetadas", detalhes); 

            dadosService.ativarConstraints(conexaoLocal);

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

            Set<String> tabelasLocal = estruturaService.obterTabelas(conexaoLocal, base);
            Set<String> tabelasCloud = estruturaService.obterTabelas(conexaoCloud, base);

            HashMap<String, List<String>> queries = estruturaService.processarTabelas(conexaoCloud, conexaoLocal, tabelasCloud, tabelasLocal, detalhes,base, nomeTabela);

            cacheService.salvarCache(base + ":" + nomeTabela, queries);
            
            response.put("tabelas_afetadas", detalhes); 
            response.put("success", true); 
        }
        catch (Exception e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        }

        return response;
    }
    public Map<String, Object> sincronizarEstrutura(String base, String nomeTabela)
    {
        Map<String, Object> response = new LinkedHashMap<>();
        List<Map<String, String>> detalhes = new ArrayList<>();

        Connection conexaoLocal = null; 
        try
        {
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);
            @SuppressWarnings("unchecked")
            HashMap<String, List<String>> querys = cacheService.buscarCache(base + ":" + nomeTabela, HashMap.class);

            if (querys == null)
            {
                response.put("success", false);
                response.put("mensagem", "Nenhuma verificação foi feita previamente.");
                return response;
            }
            
            operacaoBancoService.executarQueriesEmLotes(conexaoLocal, querys, detalhes);

            response.put("success", true); 
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

        response.put("success", false);
        response.put("error",errorType);
        response.put("message", "Erro durante sincronização");
        response.put("details", details);
    }
    
    private void finalizarConexoes(Connection conexaoCloud, Connection conexaoLocal)
    {
        try
        {
          
            if (conexaoLocal != null && !conexaoLocal.isClosed())
            {
                conexaoLocal.setAutoCommit(true); // banco faz o COMMIT automaticamente 
            }
        }
        catch (SQLException e)
        {
            System.out.println("Erro ao restaurar auto-commit "+e );
        }
        // System.out.println("fechar");
        
        ConexaoBanco.fecharTodos();
    }
     
   
}
