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
        
        for (String database : databases)
        {
            if(banco.trim().equalsIgnoreCase(database.trim()))
            {
                try
                {
                    Statement statement = abrirConexao(database);
    
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

    public  List<String> obterEstruturaTabela(String base, String banco)
    {
        List<String> listaBanco = obterBanco( base,  banco);
        
        return listaBanco;
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
