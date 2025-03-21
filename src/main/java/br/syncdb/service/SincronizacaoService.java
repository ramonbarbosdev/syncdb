package br.syncdb.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import br.syncdb.config.ConexaoBanco;
import br.syncdb.controller.TipoConexao;

@Service
public class SincronizacaoService
{
    @Autowired
    private DatabaseService databaseService;

    public  StringBuilder obterEstruturaTabela(String base, String banco )
    {
        List<String> tabelas = databaseService.obterBanco(base,  banco,TipoConexao.CLOUD );
        // List<String> tabelas = databaseService.obterBanco( base,  banco , "cloud");
        StringBuilder estuturaTabela = new StringBuilder();

        String tabelaTeste = "fonte_recurso";


        if(tabelas != null)
        {
            try
            {
                for(String tabela : tabelas)
                {
                    Connection conexao = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
                    String scriptTabela = databaseService.criarCriacaoTabelaQuery( conexao,  tabelaTeste);
                    estuturaTabela.append(scriptTabela);

                    // if (!databaseService.verificarTabelaExistente(conexaoCloud, tabela)) 
                    // {
                    
                    //     // conexaoCloud.executeUpdate(scriptTabela);
                    //     System.out.println("Tabela criada com sucesso: " + tabela);

                    // }
                

                    break;             
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
                    

        }
        
        return estuturaTabela;
    }

}
