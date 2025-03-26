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
        Map<String, Object> response = new HashMap<>();

        try
        {
            Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            Connection conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);


            if(databaseService.compararEstruturaTabela(conexaoCloud, conexaoLocal, tabela) != null)
            {
                throw new SQLException("Estrutura das tabelas divergente");
            }

            
            
            Map<String, Set<String>> dependencias = dadosService.obterDependenciasTabelas(conexaoCloud);
            
            List<String> ordemCarga = dadosService.ordenarTabelasPorDependencia(dependencias);

           for (String tabelaOrdenada : ordemCarga)
           {
              
                // String pkColumn = dadosService.obterNomeColunaPK(conexaoCloud, tabelaOrdenada);
                // long maxCloudId = dadosService.obterMaxId(conexaoCloud, tabelaOrdenada, pkColumn);
                // long maxLocalId = dadosService.obterMaxId(conexaoLocal, tabelaOrdenada, pkColumn);

                // if (maxLocalId == 0)
                // {
                //     dadosService.cargaInicialCompleta(conexaoCloud, conexaoLocal, tabelaOrdenada, response);
                // }
                // else if (maxCloudId > maxLocalId)
                // {
                //     dadosService.sincronizacaoIncremental(conexaoCloud, conexaoLocal, tabelaOrdenada, pkColumn, maxLocalId,response);
                // }
                // else
                // {
                //     // response.put("message", "Dados da tabela " + tabela + " já está sincronizada");
                //     System.out.println( "Dados da tabela " + tabelaOrdenada + " já está sincronizada");
                // }
           }

            response.put("success", true);

        }
        catch (Exception e)
        {
            response.put("success", false);
            response.put("errorType", "UNEXPECTED_ERROR");
            response.put("message", "Erro inesperado durante sincronização");
            response.put("details", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        finally
        {
            ConexaoBanco.fecharTodos();
        }
        return response;
    }
     
    public Map<String, Object> sincronizarEstrutura(String base, String nomeTabela )
    {
        Map<String, Object> response = new HashMap<>();
        try
        {

            Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            Connection conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            Set<String> tabelasLocal = estruturaService.obterTabelas(conexaoLocal, base);
            Set<String> tabelasCloud = estruturaService.obterTabelas(conexaoCloud, base);

            if(nomeTabela == null)
            {
                estruturaService.processarTabelas(conexaoCloud, conexaoLocal, tabelasCloud, tabelasLocal, response,base, null);
            }
            else
            {
                estruturaService.processarTabelas(conexaoCloud, conexaoLocal, tabelasCloud, tabelasLocal, response,base, nomeTabela);
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


   
}
