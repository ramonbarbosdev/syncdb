package br.syncdb.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

// @Configuration
public class DataSourceConfig
{
    @Bean
    @Primary /*Banco de dados original */
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties localDataSourceProperties()
    {
        return new DataSourceProperties();
    }

    @Bean
    @Primary
    public DataSource localDataSource()
    {
        return localDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean
    @Qualifier("dataSource")
    public JdbcTemplate localJdbcTemplate(@Qualifier("dataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


}
