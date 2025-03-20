package br.syncdb.config;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class DataSourceConfig
{
    @Bean
    @Primary /*Banco de dados original */
    @ConfigurationProperties("spring.datasource.local")
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
    @ConfigurationProperties("spring.datasource.cloud")
    public DataSourceProperties cloudDataSourceProperties()
    {
        return new DataSourceProperties();
    }

    @Bean
    public DataSource cloudDataSource()
    {
        return cloudDataSourceProperties().initializeDataSourceBuilder().build();
    }
    @Bean
    @ConfigurationProperties("spring.datasource.w5i")
    public DataSourceProperties w5iDataSourceProperties()
    {
        return new DataSourceProperties();
    }

    @Bean
    public DataSource w5iDataSource()
    {
        return w5iDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean
    @Qualifier("cloudDataSource")
    public JdbcTemplate cloudJdbcTemplate(@Qualifier("cloudDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @Qualifier("localDataSource")
    public JdbcTemplate localJdbcTemplate(@Qualifier("localDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    @Qualifier("w5iDataSource")
    public JdbcTemplate w5iJdbcTemplate(@Qualifier("w5iDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
