package br.syncdb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

import br.syncdb.config.DataConfigGenerico;
import br.syncdb.config.DataSourceConfig;

@Service
public class DatabaseService  
{
    
    @Autowired
    @Qualifier("cloudDataSource")
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private DataConfigGenerico dataConfigGenerico;
    
    

    public List<String> listarBases(String tipo, String nomeBase)
    {
      
            DataSource dataSource = dataConfigGenerico.createDataSource(tipo, nomeBase);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

            // // String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            String sql = "SELECT datname FROM pg_database WHERE datistemplate = false";

            List<String> listarTabelas = jdbcTemplate.queryForList(sql, String.class);;
            return listarTabelas;
        

        // return null;
        
    }

    public List<String> obterBanco(String base, String banco) 
    {
        List<String> databases = listarBases("cloud",base);
        List<String> listarTabelas = new ArrayList<>();

        if(databases == null)
        {
            return null;
        }
        
        Statement statement = abrirConexao(banco);


        for (String database : databases)
        {
            if(banco.trim().equalsIgnoreCase(database.trim()))
            {
                try
                {
    
                    String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
                    ResultSet resultSet = statement.executeQuery(query);
    
                    while (resultSet.next())
                    {
                        String tableName = resultSet.getString("table_name");
                        listarTabelas.add(tableName);
                    }
                    
                }
                catch (Exception e)
                {
                    System.out.println(e.getMessage());
                }
            }
        }

        return listarTabelas;
    }

    public  StringBuilder obterEstruturaTabela(String base, String banco)
    {
        List<String> tabelas = obterBanco( base,  banco);
        StringBuilder estuturaTabela = new StringBuilder();

        String tabelaTeste = "operacao_lote";


        if(tabelas != null)
        {
            Statement statement = abrirConexao(banco);

            for(String tabela : tabelas)
            {
               try
               {
                    String scriptTabela = criarCriacaoTabelaQuery( statement,  tabelaTeste);
                    estuturaTabela.append(scriptTabela);
                    
                    break;

               }
               catch (Exception e)
               {
                // TODO: handle exception
               }
                                
            }
        }
        
        return estuturaTabela;
    }

    public static String criarCriacaoTabelaQuery( Statement statement ,String tabelaOrigem)
    {
        StringBuilder createTableScript = new StringBuilder();

        createTableScript.append("CREATE TABLE " + tabelaOrigem + " (\n");

        try 
        {

            String query = "SELECT column_name, data_type, is_nullable, column_default " +
                           "FROM information_schema.columns " +
                           "WHERE table_name = '" + tabelaOrigem + "' " +
                           "AND table_schema = 'public';";

            ResultSet resultadoQuery = statement.executeQuery(query);

            while (resultadoQuery.next()) {
                String nomeColuna = resultadoQuery.getString("column_name");
                String tipoColuna = resultadoQuery.getString("data_type");
                String nullable = resultadoQuery.getString("is_nullable");
                String defaultColuna = resultadoQuery.getString("column_default");

                createTableScript.append("    ").append(nomeColuna).append(" ").append(tipoColuna);

                if ("NO".equalsIgnoreCase(nullable)) {
                    createTableScript.append(" NOT NULL");
                }

                if (defaultColuna != null) {
                    createTableScript.append(" DEFAULT ").append(defaultColuna);
                }

                createTableScript.append(",\n");
            }

   
            if (createTableScript.length() > 2)
            {
                createTableScript.setLength(createTableScript.length() - 2); 
            }

            createTableScript.append("\n);\n");

            // obter chaves estrangeiras
            String foreignKeyQuery = "SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name " +
                                      "FROM information_schema.table_constraints AS tc " +
                                      "JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name " +
                                      "JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name " +
                                      "WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name = '" + tabelaOrigem + "';";

            ResultSet foreignKeyResultSet = statement.executeQuery(foreignKeyQuery);
            while (foreignKeyResultSet.next()) {
                String constraintName = foreignKeyResultSet.getString("constraint_name");
                String columnName = foreignKeyResultSet.getString("column_name");
                String foreignTableName = foreignKeyResultSet.getString("foreign_table_name");
                String foreignColumnName = foreignKeyResultSet.getString("foreign_column_name");

                createTableScript.append("ALTER TABLE ").append(tabelaOrigem)
                                 .append(" ADD CONSTRAINT ").append(constraintName)
                                 .append(" FOREIGN KEY (").append(columnName)
                                 .append(") REFERENCES ").append(foreignTableName)
                                 .append("(").append(foreignColumnName).append(");\n");
            }

            // obter índices
            String indexQuery = "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '" + tabelaOrigem + "';";
            ResultSet indexResultSet = statement.executeQuery(indexQuery);
            while (indexResultSet.next()) {
                String indexName = indexResultSet.getString("indexname");
                String indexDef = indexResultSet.getString("indexdef");

                createTableScript.append(indexDef).append(";\n");
            }

        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

        return createTableScript.toString();
    }
   
    public Statement abrirConexao(String database) 
    {

        try
        {
            String host = "db-postgresql-nyc3-32073-do-user-9424476-0.b.db.ondigitalocean.com";
            String port = "25060";
            String user = "doadmin";
            String password = "AVNS_ThcFV7CzqE1EzYP7W8z";
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + database; 
    
            Connection connection = DriverManager.getConnection( url, user, password);
            Statement statement = (Statement) connection.createStatement(); 
            return statement;

        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        
        return null;

    }

}
