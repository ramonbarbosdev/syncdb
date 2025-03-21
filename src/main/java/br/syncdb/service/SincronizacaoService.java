package br.syncdb.service;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SincronizacaoService
{
    @Autowired
    private DatabaseService databaseService;

    public  StringBuilder obterEstruturaTabela(String base, String banco )
    {
        List<String> tabelas = databaseService.obterBanco(base,  banco,"cloud"  );
        // List<String> tabelas = databaseService.obterBanco( base,  banco , "cloud");
        StringBuilder estuturaTabela = new StringBuilder();

        String tabelaTeste = "fonte_recurso";


        if(tabelas != null)
        {
            Statement conexaoCloud = databaseService.abrirConexao(banco);

            for(String tabela : tabelas)
            {
               try
               {
                    String scriptTabela = databaseService.criarCriacaoTabelaQuery( conexaoCloud,  tabelaTeste);
                    estuturaTabela.append(scriptTabela);

                    // if (!databaseService.verificarTabelaExistente(conexaoCloud, tabela)) 
                    // {
                    
                    //     // conexaoCloud.executeUpdate(scriptTabela);
                    //     System.out.println("Tabela criada com sucesso: " + tabela);

                    // }
             
                    
                
                    
                    break;

               }
                catch (Exception e) {
                    e.printStackTrace();
                }
                                
            }
        }
        
        return estuturaTabela;
    }

}
