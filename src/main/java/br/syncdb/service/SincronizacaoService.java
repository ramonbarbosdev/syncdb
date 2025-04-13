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

import br.syncdb.config.CacheManager;
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


    private static final Map<String, Map<String, Object>> cache = new ConcurrentHashMap<>();

   

    public Map<String, Object> sincronizarDados(String base, String tabela, Boolean fl_verificacao)
    {
       
        Connection conexaoCloud = null;
        Connection conexaoLocal = null;
        Map<String, Object> response = new HashMap<String, Object>();
        List<TabelaDetalhe> detalhes = new ArrayList<>();
        Map<String,List<String>> querys =  new LinkedHashMap<>(); 
       
        try
        {
            conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);
            conexaoLocal.setAutoCommit(false);

            dadosService.desativarConstraints(conexaoLocal);

            dadosService.obterDadosTabelasPendentesCriacao(conexaoCloud,conexaoLocal, tabela, detalhes, querys );

            response.put("tabelas_afetadas", detalhes); 

            if(fl_verificacao == false)
            {
                operacaoBancoService.execultarQuerySQL(conexaoLocal,querys);
                response.put("message", "Sincronização de dados concluida"); 
            }
            
            response.put("success", true); 
            dadosService.ativarConstraints(conexaoLocal);
            conexaoLocal.commit();

        }
        catch (DataAccessException e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        }
        catch (Exception e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        }
        finally
        {
            finalizarConexoes(conexaoCloud, conexaoLocal);
        }
        
        return response;
    }
    
    

    public Map<String, Object> sincronizarEstrutura(String base, String nomeTabela, boolean fl_verificacao)
    {
        Map<String, Object> response = new LinkedHashMap<>(); 
        List<EstruturaTabela> detalhes = new ArrayList<>();
        
        Connection conexaoCloud = null;
        Connection conexaoLocal = null; 
        try
        {
            conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            conexaoLocal.setAutoCommit(false);

            Set<String> tabelasLocal = estruturaService.obterTabelas(conexaoLocal, base);
            Set<String> tabelasCloud = estruturaService.obterTabelas(conexaoCloud, base);

            HashMap<String, List<String>> queries = estruturaService.processarTabelas(conexaoCloud, conexaoLocal, tabelasCloud, tabelasLocal, detalhes,base, nomeTabela);
            
            if(fl_verificacao == false)
            {
                estruturaService.executarQueriesEmLotes(conexaoLocal, queries, detalhes);
                response.put("fl_verificacao", fl_verificacao); 
            }
            else
            {
                response.put("fl_verificacao", fl_verificacao); 
            }

            response.put("tabelas_afetadas", detalhes); 
            response.put("success", true); 
            conexaoLocal.commit();
        }
        catch (SQLException e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        
        }
        catch (Exception e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
            
        } finally
        {
            finalizarConexoes(conexaoCloud, conexaoLocal);
        }

        return response;
    }

    
    private void    tratarErroSincronizacao(Map<String, Object> response, Connection conexaoLocal, Exception e)
    {
        if (conexaoLocal != null)
        {
            try
            {
                conexaoLocal.rollback();
                System.out.println("rollback");

            } catch (SQLException ex)
            {
                System.out.println("Erro ao fazer rollback "+ex );
            }
        }
        
        String errorType = e.getClass().getSimpleName();
        String details = e.getMessage();

        if( details.contains("does not exist"))
        {
            details = "A base informada não existe no servidor local.";
        }

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
