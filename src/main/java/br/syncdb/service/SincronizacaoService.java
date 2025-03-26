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

    @Autowired
    private DadosService dadosService;


    public Map<String, Object> sincronizarDados(String base, String tabela)
    {
        Map<String, Object> response = new LinkedHashMap<>(); 
        Connection conexaoCloud = null;
        Connection conexaoLocal = null;
     
        try
        {
            conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            conexaoLocal.setAutoCommit(false); //banco NÃO faz o COMMIT automaticamente 

            dadosService.desativarConstraints(conexaoLocal);

            processarSincronizacao(conexaoCloud, conexaoLocal, tabela, response);
            
            conexaoLocal.commit();
            response.put("success", true);
            
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
    
    private void processarSincronizacao(Connection conexaoCloud, Connection conexaoLocal, 
                                       String tabela, Map<String, Object> response) throws SQLException
    {
        estruturaService.validarEstruturaTabela(conexaoCloud, conexaoLocal, tabela);
        
        
        Map<String, Set<String>> dependencias = dadosService.obterDependenciasTabelas(conexaoCloud);

        List<String> ordemCarga = dadosService.ordenarTabelasPorDependencia(dependencias);

        if (tabela != null)
        {
            ordemCarga = dadosService.filtrarTabelasRelevantes(tabela, ordemCarga, dependencias);
        }
     
        dadosService.processarCargaTabelas(conexaoCloud, conexaoLocal, ordemCarga, response);
        
        // dadosService.validarIntegridadeDados(conexaoLocal, response);
    }
    

    public Map<String, Object> sincronizarEstrutura(String base, String nomeTabela )
    {
        Map<String, Object> response = new LinkedHashMap<>(); 

        Connection conexaoCloud = null;
        Connection conexaoLocal = null;
        try
        {
             conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
             conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            Set<String> tabelasLocal = estruturaService.obterTabelas(conexaoLocal, base);
            Set<String> tabelasCloud = estruturaService.obterTabelas(conexaoCloud, base);

            estruturaService.processarTabelas(conexaoCloud, conexaoLocal, tabelasCloud, tabelasLocal, response,base, nomeTabela);
           
        }
        catch (SQLException e)
        {
            tratarErroSincronizacao(response, conexaoLocal, e);
        
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
            finalizarConexoes(conexaoCloud, conexaoLocal);
        }

        return response;
    }

    
    private void tratarErroSincronizacao(Map<String, Object> response, Connection conexaoLocal, Exception e)
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
        
        response.put("success", false);
        response.put("errorType", e.getClass().getSimpleName());
        response.put("message", "Erro durante sincronização");
        response.put("details", e.getMessage());
        
        // System.out.println("Erro na sincronização "+e );

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
        ConexaoBanco.fecharConexao(base);
    }
     
   
}
