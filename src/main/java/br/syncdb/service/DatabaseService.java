package br.syncdb.service;

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

    public List<String> listarTabelasPorBase(String nomeBase)
    {
        if(verificarBase(nomeBase))
        {
            DataSource dataSource = dataConfigGenerico.createDataSource(nomeBase);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

            String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";

            return jdbcTemplate.queryForList(sql, String.class);
        }

        return null;
        
    }

    public List<String> listDatabases()
    {
        String sql = "SELECT datname FROM pg_database WHERE datistemplate = false";

        return jdbcTemplate.queryForList(sql, String.class);
    }

    
    public boolean verificarBase(String nomeBase)
    {
        List<String> databases = listDatabases();

        for (String db : databases)
        {
            if (db.equals(nomeBase))
            {
                return true;
            }
        }

        return false;
    }

    public List<String> sincronizarBase(String nomeBase)
    {
        String sql = "SELECT table_catalog FROM information_schema.tables  ";

        Object[] args = { nomeBase };

        List<String> list = jdbcTemplate.queryForList(sql,args, String.class);

        return list;
    }
}
