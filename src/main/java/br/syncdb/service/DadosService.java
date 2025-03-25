package br.syncdb.service;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.stereotype.Service;

@Service
public class DadosService {
    
    /**
     * Obtém o valor máximo da coluna especificada em uma tabela
     * @param conn Conexão com o banco de dados
     * @param tabela Nome da tabela
     * @param nomeColuna Nome da coluna (normalmente a PK)
     * @return Valor máximo da coluna, ou 0 se a tabela estiver vazia
     * @throws SQLException
     */
    public long obterMaxId(Connection conn, String tabela, String nomeColuna) throws SQLException {
        // Se a coluna não for especificada, tenta obter a PK automaticamente
        if (nomeColuna == null || nomeColuna.isEmpty()) {
            nomeColuna = obterColunaPK(conn, tabela);
            if (nomeColuna == null) {
                throw new SQLException("Não foi possível identificar a coluna para obter o máximo ID");
            }
        }

        String sql = String.format("SELECT COALESCE(MAX(%s), 0) FROM %s", nomeColuna, tabela);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery()) {
            
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

/**
 * Método auxiliar para obter o nome da coluna de chave primária
 */
    public String obterColunaPK(Connection conn, String tabela) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, tabela)) {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
            return null;
        }
    }

    public void cargaInicialCompleta(Connection cloudConn, Connection localConn, String tabela) throws SQLException {
        final int BATCH_SIZE = 1000;
        final int PAGE_SIZE = 50000;
        long offset = 0;
        
        try (java.sql.Statement cloudStmt = cloudConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
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
                
                        PreparedStatement localInsert = criarPreparedInsert(localConn, tabela, rs.getMetaData());

                        int batchCount = 0;
                        localConn.setAutoCommit(false);

                        while (rs.next())
                        {
                            preencherPreparedStatement(localInsert, rs);
                            localInsert.addBatch();

                            if (++batchCount % BATCH_SIZE == 0)
                            {
                                localInsert.executeBatch();
                                localConn.commit();
                            }
                        }

                        if (batchCount > 0)
                        {
                            localInsert.executeBatch();
                            localConn.commit();
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
        }
        catch (SQLException e)
        {
            localConn.rollback();
            throw e;
        }
    }
    
    public void sincronizacaoIncremental(Connection cloudConn, Connection localConn, 
                                        String tabela, String pkColumn, long maxLocalId) throws SQLException
    {
        final int BATCH_SIZE = 1000;
        
        String query = String.format("SELECT * FROM %s WHERE %s > ? ORDER BY %s", 
                                tabela, pkColumn, pkColumn);
        
        try (PreparedStatement cloudStmt = cloudConn.prepareStatement(query, 
                                        ResultSet.TYPE_FORWARD_ONLY, 
                                        ResultSet.CONCUR_READ_ONLY))
        {
            cloudStmt.setFetchSize(1000);
            cloudStmt.setLong(1, maxLocalId);
            
            try (ResultSet rs = cloudStmt.executeQuery();
                PreparedStatement localInsert = criarPreparedInsert(localConn, tabela, rs.getMetaData()))
            {
                
                localConn.setAutoCommit(false);
                int batchCount = 0;
                
                while (rs.next())
                {
                    preencherPreparedStatement(localInsert, rs);
                    localInsert.addBatch();
                    
                    if (++batchCount % BATCH_SIZE == 0)
                    {
                        localInsert.executeBatch();
                        localConn.commit();
                    }
                }
                
                localInsert.executeBatch();
                localConn.commit();
            }
        }
    }

    public PreparedStatement criarPreparedInsert(Connection conn, String tabela, ResultSetMetaData meta) throws SQLException
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
        
        return conn.prepareStatement(sql.toString());
    }

    public void preencherPreparedStatement(PreparedStatement stmt, ResultSet rs) throws SQLException
    {
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
        {
            stmt.setObject(i, rs.getObject(i));
        }
    }

}
