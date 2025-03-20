package br.syncdb.config;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
public class DataConfigGenerico {

    @Value("${spring.datasource.cloud.username}")
    private String username;

    @Value("${spring.datasource.cloud.password}")
    private String password;

    @Value("${spring.datasource.cloud.driver-class-name}")
    private String driverClassName;

    // Método para criar um DataSource dinâmico
    public DataSource createDataSource(String databaseName) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl("jdbc:postgresql://db-postgresql-nyc3-32073-do-user-9424476-0.b.db.ondigitalocean.com:25060/" + databaseName);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }
}