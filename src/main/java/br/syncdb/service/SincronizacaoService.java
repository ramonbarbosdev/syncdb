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
            //cloud
            Set<String> nomeTabelaCloud =  obterMetaData( base,  TipoConexao.CLOUD);
            Set<String> nomeTabelaLocal =  obterMetaData( base,  TipoConexao.LOCAL);
           

            for (String nomeTabela : nomeTabelaCloud)
            {
                if (!nomeTabelaLocal.contains(nomeTabela))
                {
                    System.out.println("Tabela " + nomeTabela + " não existe no banco local.");
                    // Aqui você pode criar a tabela no banco local se necessário
                }
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
              
    }

    public  Set<String>  obterMetaData(String base, TipoConexao tipo)
    {
        try
        {
            Connection conexao = ConexaoBanco.abrirConexao(base, tipo);

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

    public Map<String, String> obterEstruturaTabela(String base, String banco)
    {
        List<String> tabelas = databaseService.obterBanco(base, banco, TipoConexao.CLOUD);
        Map<String, String> estruturaTabela = new HashMap<>(); 

        if (tabelas != null) {
            try {
                Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);

                for (String tabela : tabelas)
                {
                    String scriptTabela = databaseService.criarCriacaoTabelaQuery(conexaoCloud, tabela);

                    estruturaTabela.put(tabela, scriptTabela); 

              
                    break;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            finally
            {
                ConexaoBanco.fecharConexao(base);
            }
        }

        return estruturaTabela; // Retorna o Map com as tabelas e suas consultas
    }

}
