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
import java.util.Set;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import br.syncdb.config.ConexaoBanco;
import br.syncdb.controller.TipoConexao;

@Service
public class SincronizacaoService
{
    @Autowired
    private DatabaseService databaseService;



    public   void executarCriacaoTabela(String base, String banco)
    {
        try
        {
            Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            Connection conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            Set<String> nomeTabelaCloud =  obterMetaData( base, conexaoCloud);
            Set<String> nomeTabelaLocal =  obterMetaData( base, conexaoLocal);

           
            StringBuilder queryCriacaoTabela = new StringBuilder();
            StringBuilder queryChaveSecundaria = new StringBuilder();
            StringBuilder queryIndices = new StringBuilder();
            int contagemProcesso = 1;
            for (String nomeTabela : nomeTabelaCloud)
            {
                if (!nomeTabelaLocal.contains(nomeTabela))
                {
                    if(nomeTabela.contains("system_users"))
                    {
                        
                    }
                    String criacaoTabela = databaseService.obterEstruturaTabela(conexaoCloud, nomeTabela);
                    // String chaveEstrangeira = databaseService.obterChaveEstrangeira(conexaoCloud, nomeTabela);
                    // String indices = databaseService.obterIndices(conexaoCloud, nomeTabela);
                    
                    queryCriacaoTabela.append(criacaoTabela.toString() );
                    // queryChaveSecundaria.append(chaveEstrangeira.toString() );
                    // queryIndices.append(indices.toString() );
                    // var stmt = conexaoLocal.createStatement();
                    // stmt.executeUpdate(estruturaTabela);
                    System.out.println("Processo: " + contagemProcesso+"/"+nomeTabelaCloud.size());
                    contagemProcesso++;
                   
                   
                }
                
            }

            try (Statement stmt = conexaoLocal.createStatement())
            {
                if (queryCriacaoTabela.length() > 0)
                {
                    stmt.executeUpdate(queryCriacaoTabela.toString());
                    System.out.println("Tabelas criadas com sucesso.");
                }
    
                // if (queryChaveSecundaria.length() > 0)
                // {
                //     stmt.executeUpdate(queryChaveSecundaria.toString());
                //     System.out.println("Chaves estrangeiras adicionadas com sucesso.");
                // }
    
                // if (queryIndices.length() > 0)
                // {
                //     stmt.executeUpdate(queryIndices.toString());
                //     System.out.println("Índices adicionados com sucesso.");
                // }
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            ConexaoBanco.fecharConexao(base);
        }
              
    }


    public  Set<String>  obterMetaData(String base, Connection conexao)
    {
        try
        {
            if( conexao == null)
            {
                return null;
            }

            DatabaseMetaData conexaoMetaData = conexao.getMetaData();
            ResultSet tabelas = conexaoMetaData.getTables(null, null, "%", new String[] {"TABLE"});
            
            Set<String> nomeTabelas = new HashSet<>();
            while (tabelas.next())
            {
                nomeTabelas.add(tabelas.getString("TABLE_NAME"));
            }

            return nomeTabelas;
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            ConexaoBanco.fecharConexao(base);
        }
    }

}
