package br.syncdb.service;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.stereotype.Service;

@Service
public class DadosService {
    
    
    public long obterMaxId(Connection conexao, String tabela, String nomeColuna) throws SQLException
    {
       
        if (nomeColuna == null || nomeColuna.isEmpty())
        {
            nomeColuna = obterNomeColunaPK(conexao, tabela);
            if (nomeColuna == null) {
                throw new SQLException("Não foi possível identificar a coluna para obter o máximo ID");
            }
        }

        String sql = String.format("SELECT COALESCE(MAX(%s), 0) FROM %s", nomeColuna, tabela);
        
        try (PreparedStatement stmt = conexao.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery())
            {
            
            if (rs.next())
            {
                return rs.getLong(1);
            }
            return 0;
        }
    }


    public String obterNomeColunaPK(Connection conexao, String tabela) throws SQLException
    {
        try (ResultSet rs = conexao.getMetaData().getPrimaryKeys(null, null, tabela))
        {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
            return null;
        }
    }

    public void cargaInicialCompleta(Connection conexaoCloud, Connection conexaoLocal, String tabela, Map<String, Object> response) throws SQLException
    {
        final int BATCH_SIZE = 1000;
        final int PAGE_SIZE = 50000;
        long offset = 0;
        
        try (java.sql.Statement cloudStmt = conexaoCloud.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
        {
            cloudStmt.setFetchSize(BATCH_SIZE);

            while (true)
            {
                String query = String.format("SELECT * FROM %s ORDER BY 1 LIMIT %d OFFSET %d", 
                                              tabela, PAGE_SIZE, offset);

                try (ResultSet rs = cloudStmt.executeQuery(query))
                {
                    if (rs.isBeforeFirst())
                    {
                
                        PreparedStatement localInsert = criarPreparedInsert(conexaoLocal, tabela, rs.getMetaData());

                        int batchCount = 0;
                        conexaoLocal.setAutoCommit(false);

                        while (rs.next())
                        {
                            preencherPreparedStatement(localInsert, rs);
                            localInsert.addBatch();

                            if (++batchCount % BATCH_SIZE == 0)
                            {
                                localInsert.executeBatch();
                                conexaoLocal.commit();
                            }
                        }

                        if (batchCount > 0)
                        {
                            localInsert.executeBatch();
                            conexaoLocal.commit();
                        }

                        localInsert.close();

                        if (batchCount < PAGE_SIZE) break;

                        offset += PAGE_SIZE;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            response.put("message", "Sincronização concluida.");

        }
        catch (SQLException e)
        {
            conexaoLocal.rollback();
            throw e;
        }
    }
    
    public void sincronizacaoIncremental(Connection conexaoCloud, Connection conexaoLocal, 
        String tabela,
        String pkColumn,
        long maxLocalId,
        Map<String, Object> response
    ) throws SQLException
    {
        final int BATCH_SIZE = 1000;
        
        String query = String.format("SELECT * FROM %s WHERE %s > ? ORDER BY %s", 
                                tabela, pkColumn, pkColumn);
        
        try (PreparedStatement cloudStmt = conexaoCloud.prepareStatement(query,  ResultSet.TYPE_FORWARD_ONLY,  ResultSet.CONCUR_READ_ONLY))
        {
            cloudStmt.setFetchSize(1000);
            cloudStmt.setLong(1, maxLocalId);
            
            try (ResultSet rs = cloudStmt.executeQuery();  PreparedStatement localInsert = criarPreparedInsert(conexaoLocal, tabela, rs.getMetaData()))
            {
                
                conexaoLocal.setAutoCommit(false);
                int batchCount = 0;
                
                while (rs.next())
                {
                    preencherPreparedStatement(localInsert, rs);
                    localInsert.addBatch();
                    
                    if (++batchCount % BATCH_SIZE == 0)
                    {
                        localInsert.executeBatch();
                        conexaoLocal.commit();
                    }
                }
                
                localInsert.executeBatch();
                conexaoLocal.commit();
                response.put("message", "Sincronização incremental concluida.");

            }
        }
    }

    public PreparedStatement criarPreparedInsert(Connection conexao, String tabela, ResultSetMetaData meta) throws SQLException
    {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tabela).append(" (");
        StringBuilder values = new StringBuilder("VALUES (");
        
        int columnCount = meta.getColumnCount();
        for (int i = 1; i <= columnCount; i++)
        {
            if (i > 1)
            {
                sql.append(", ");
                values.append(", ");
            }
            sql.append(meta.getColumnName(i));
            values.append("?");
        }
        
        sql.append(") ").append(values).append(")");
        
        return conexao.prepareStatement(sql.toString());
    }

    public void preencherPreparedStatement(PreparedStatement stmt, ResultSet rs) throws SQLException
    {
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
        {
            stmt.setObject(i, rs.getObject(i));
        }
    }

}
