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
        Connection conexaoCloud = null;
        Connection conexaoLocal = null;
        
        try
        {
            conexaoCloud = ConexaoBanco.abrirConexao(base, TipoConexao.CLOUD);
            conexaoLocal = ConexaoBanco.abrirConexao(base, TipoConexao.LOCAL);
            
            // Obter dados das tabelas em paralelo (se possível)
            Set<String> nomeTabelaCloud =  databaseService.obterTabelaMetaData(base, conexaoCloud);
            Set<String> nomeTabelaLocal =  databaseService.obterTabelaMetaData(base, conexaoLocal);
    
            // Usando um StringBuilder para armazenar todas as queries de forma eficiente
            StringBuilder querySequencia = new StringBuilder();
            StringBuilder queryFuncoes = new StringBuilder();
            StringBuilder queryCriacaoTabela = new StringBuilder();
            StringBuilder queryChaveSecundaria = new StringBuilder();
            StringBuilder queryAlteracoes = new StringBuilder();
    
            // Iniciar transação no banco local
            conexaoLocal.setAutoCommit(false);
    
            int contagemProcesso = 1;
            
            // Usar execução em batch para melhorar o desempenho
            for (String nomeTabela : nomeTabelaCloud)
            {
                try {
                    if (!nomeTabelaLocal.contains(nomeTabela))
                    {
                        String criacaoSequencia = databaseService.criarSequenciaQuery(conexaoCloud);
                        String criacaoTabela = databaseService.obterEstruturaTabela(conexaoCloud, nomeTabela);
                        String chaveEstrangeira = databaseService.obterChaveEstrangeira(conexaoCloud, nomeTabela);
    
                        // Acumular queries
                        querySequencia.append(criacaoSequencia);
                        queryCriacaoTabela.append(criacaoTabela);
                        queryChaveSecundaria.append(chaveEstrangeira);
                    }
                    else
                    {
                        String criacaoFuncoes = databaseService.criarFuncoesQuery(conexaoCloud);
                        queryFuncoes.append(criacaoFuncoes);
    
                        // Comparar e preparar alterações de estrutura de tabela
                        String alteracoes = compararEstruturaTabela(conexaoCloud, conexaoLocal, nomeTabela);
                        queryAlteracoes.append(alteracoes);
                    }
    
                    contagemProcesso++;
                    System.out.println(contagemProcesso + "/" + nomeTabelaCloud.size());
                }
                catch (SQLException ex)
                {
                    ex.printStackTrace();
                    // Aguardar, ou salvar em log, se desejar, e continuar
                }
            }
    
            // Executar todas as queries de forma otimizada
            try (Statement stmt = conexaoLocal.createStatement()) {
                // Executar as queries em batch
                if (querySequencia.length() > 0)
                {
                    stmt.addBatch(querySequencia.toString());
                    System.out.println("Sequências preparadas.");
                }
    
                if (queryFuncoes.length() > 0)
                {
                    stmt.addBatch(queryFuncoes.toString());
                    System.out.println("Funções preparadas.");
                }
    
                if (queryCriacaoTabela.length() > 0)
                {
                    stmt.addBatch(queryCriacaoTabela.toString());
                    System.out.println("Tabelas preparadas.");
                }
    
                if (queryAlteracoes.length() > 0)
                {
                    stmt.addBatch(queryAlteracoes.toString());
                    System.out.println("Alterações preparadas.");
                }
    
                if (queryChaveSecundaria.length() > 0)
                {
                    stmt.addBatch(queryChaveSecundaria.toString());
                    System.out.println("Chaves estrangeiras preparadas.");
                }
    
                // Executar o batch
                stmt.executeBatch();
                conexaoLocal.commit();
                System.out.println("Processo concluído com sucesso.");
            }
            catch (SQLException e)
            {
                conexaoLocal.rollback(); // Reverter em caso de erro
                e.printStackTrace();
            }
        }
        catch (SQLException e)
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
