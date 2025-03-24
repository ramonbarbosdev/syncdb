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
import java.util.Objects;
import java.util.Set;
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



    public void executarCriacaoTabela(String base, String banco)
    {
        try
        {
            Connection conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            Connection conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);

            Set<String> nomeTabelaCloud =  databaseService.obterTabelaMetaData( base, conexaoCloud);
            Set<String> nomeTabelaLocal =  databaseService.obterTabelaMetaData( base, conexaoLocal);

           
            StringBuilder querySequencia = new StringBuilder();
            StringBuilder queryCriacaoTabela = new StringBuilder();
            StringBuilder queryChaveSecundaria = new StringBuilder();
            StringBuilder queryAlteracoes = new StringBuilder();
            int contagemProcesso = 1;

            for (String nomeTabela : nomeTabelaCloud)
            {
                if (!nomeTabelaLocal.contains(nomeTabela))
                {
                    String criacaoSequncia = databaseService.criarSequenciaQuery(conexaoCloud,nomeTabela );
                    String criacaoTabela = databaseService.obterEstruturaTabela(conexaoCloud, nomeTabela);
                    String chaveEstrangeira = databaseService.obterChaveEstrangeira(conexaoCloud, nomeTabela);
                    
                    querySequencia.append(criacaoSequncia.toString() );
                    queryCriacaoTabela.append(criacaoTabela.toString() );
                    queryChaveSecundaria.append(chaveEstrangeira.toString() );
                   
                }
                else
                {
                  
                    // 🏆 Tabela existe → comparar estrutura das colunas
                    String alteracoes = compararEstruturaTabela(conexaoCloud, conexaoLocal, nomeTabela);
                    queryAlteracoes.append(alteracoes);
                }
                contagemProcesso++;
                System.out.println(contagemProcesso+"/"+nomeTabelaCloud.size());
                
            }

            try (Statement stmt = conexaoLocal.createStatement())
            {
                if (querySequencia.length() > 0)
                {
                    stmt.executeUpdate(querySequencia.toString());
                    System.out.println("Sequencias criadas com sucesso.");
                }
                
                if (queryCriacaoTabela.length() > 0)
                {
                    stmt.executeUpdate(queryCriacaoTabela.toString());
                    System.out.println("Tabelas criadas com sucesso.");
                }

                if (queryAlteracoes.length() > 0) {
                    stmt.executeUpdate(queryAlteracoes.toString());
                    System.out.println("Estruturas das tabelas sincronizadas com sucesso.");
                }
    
                if (queryChaveSecundaria.length() > 0)
                {
                    stmt.executeUpdate(queryChaveSecundaria.toString());
                    System.out.println("Chaves estrangeiras adicionadas com sucesso.");
                }
    
               
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
    private String compararEstruturaTabela(Connection conexaoCloud, Connection conexaoLocal, String nomeTabela) throws SQLException {
        StringBuilder alteracoes = new StringBuilder();
    
        DatabaseMetaData metaDataCloud = conexaoCloud.getMetaData();
        DatabaseMetaData metaDataLocal = conexaoLocal.getMetaData();
    
        // ✅ Obter colunas da tabela na origem (Cloud)
        ResultSet colunasCloud = metaDataCloud.getColumns(null, "public", nomeTabela, null);
        Map<String, Coluna> estruturaCloud = new HashMap<>();
    
        while (colunasCloud.next()) {
            Coluna coluna = new Coluna();
            coluna.setNome(colunasCloud.getString("COLUMN_NAME"));
            coluna.setTipo(colunasCloud.getString("TYPE_NAME"));
            coluna.setNullable(colunasCloud.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
            coluna.setDefaultValor(colunasCloud.getString("COLUMN_DEF"));
    
            estruturaCloud.put(coluna.getNome(), coluna);
        }
    
        colunasCloud.close();
    
        // ✅ Obter colunas da tabela no destino (Local)
        ResultSet colunasLocal = metaDataLocal.getColumns(null, "public", nomeTabela, null);
        Map<String, Coluna> estruturaLocal = new HashMap<>();
    
        while (colunasLocal.next()) {
            Coluna coluna = new Coluna();
            coluna.setNome(colunasLocal.getString("COLUMN_NAME"));
            coluna.setTipo(colunasLocal.getString("TYPE_NAME"));
            coluna.setNullable(colunasLocal.getInt("NULLABLE") == DatabaseMetaData.columnNullable);
            coluna.setDefaultValor(colunasLocal.getString("COLUMN_DEF"));
    
            estruturaLocal.put(coluna.getNome(), coluna);
        }
    
        colunasLocal.close();
    
        // ✅ Adicionar novas colunas ou alterar colunas existentes
        for (String nomeColuna : estruturaCloud.keySet()) {
            Coluna colunaCloud = estruturaCloud.get(nomeColuna);
            Coluna colunaLocal = estruturaLocal.get(nomeColuna);
    
            if (colunaLocal == null) {
                // 🔥 Nova coluna → Adicionar ao destino
                alteracoes.append("ALTER TABLE ")
                          .append(nomeTabela)
                          .append(" ADD COLUMN ")
                          .append(colunaCloud.getNome()).append(" ")
                          .append(colunaCloud.getTipo());
    
                if (!colunaCloud.isNullable()) {
                    alteracoes.append(" NOT NULL");
                }
    
                if (colunaCloud.getDefaultValor() != null) {
                    alteracoes.append(" DEFAULT ").append(colunaCloud.getDefaultValor());
                }
    
                alteracoes.append(";\n");
            } else {
                // 🚀 Coluna existe → Verificar diferenças
                if (!colunaCloud.getTipo().equalsIgnoreCase(colunaLocal.getTipo())) {
                    alteracoes.append("ALTER TABLE ")
                              .append(nomeTabela)
                              .append(" ALTER COLUMN ")
                              .append(nomeColuna)
                              .append(" TYPE ")
                              .append(colunaCloud.getTipo())
                              .append(";\n");
                }
    
                if (colunaCloud.isNullable() != colunaLocal.isNullable()) {
                    alteracoes.append("ALTER TABLE ")
                              .append(nomeTabela)
                              .append(" ALTER COLUMN ")
                              .append(nomeColuna)
                              .append(colunaCloud.isNullable() ? " DROP NOT NULL" : " SET NOT NULL")
                              .append(";\n");
                }
    
                if ((colunaCloud.getDefaultValor() == null && colunaLocal.getDefaultValor() != null) ||
                    (colunaCloud.getDefaultValor() != null && !colunaCloud.getDefaultValor().equals(colunaLocal.getDefaultValor()))) {
                    if (colunaCloud.getDefaultValor() == null) {
                        alteracoes.append("ALTER TABLE ")
                                  .append(nomeTabela)
                                  .append(" ALTER COLUMN ")
                                  .append(nomeColuna)
                                  .append(" DROP DEFAULT")
                                  .append(";\n");
                    } else {
                        alteracoes.append("ALTER TABLE ")
                                  .append(nomeTabela)
                                  .append(" ALTER COLUMN ")
                                  .append(nomeColuna)
                                  .append(" SET DEFAULT ")
                                  .append(colunaCloud.getDefaultValor())
                                  .append(";\n");
                    }
                }
            }
        }
    
        // ✅ Remover colunas que não existem mais
        for (String nomeColuna : estruturaLocal.keySet()) {
            if (!estruturaCloud.containsKey(nomeColuna)) {
                alteracoes.append("ALTER TABLE ")
                          .append(nomeTabela)
                          .append(" DROP COLUMN ")
                          .append(nomeColuna)
                          .append(";\n");
            }
        }
    
        return alteracoes.toString();
    }
    
    
    // 🏆 Método para obter a estrutura de colunas
    
    

   
}
