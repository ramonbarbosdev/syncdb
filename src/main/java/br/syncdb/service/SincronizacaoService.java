package br.syncdb.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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



    public   Map<String, String> executarCriacaoTabela(String base, String banco)
    {
        try
        {
            Map<String, String> estruturaTabelas = obterEstruturaTabela(base, banco);

            if(estruturaTabelas == null)
            {
                return null;
            }
  
            for(  Entry<String, String> entry : estruturaTabelas.entrySet())
            {
                String tabelaName = entry.getKey();
                String scriptTabela = entry.getValue();
    
    
                if (!databaseService.verificarTabelaExistente(base, TipoConexao.LOCAL, tabelaName))
                {
                    Connection conexao = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);
    
                    conexao.createStatement().executeUpdate(scriptTabela);
                    System.out.println("Tabela criada com sucesso: " + tabelaName);
                }
    
            }

            return estruturaTabelas;

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            ConexaoBanco.fecharConexao(base);
        }

        return null;
      
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
