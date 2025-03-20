package br.syncdb.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

@Service
public class DatabaseService  
{
    
    @Autowired
    @Qualifier("cloudDataSource")
    private JdbcTemplate jdbcTemplate;

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
}
