package br.syncdb.config;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
public class DataConfigGenerico {

    // @Value("${username.banco}")
    // private String username;

    // @Value("${password.banco}")
    // private String password;

    private String username = "doadmin";
    private String password = "AVNS_ThcFV7CzqE1EzYP7W8z";
    private String cloudUrl = "jdbc:postgresql://db-postgresql-nyc3-32073-do-user-9424476-0.b.db.ondigitalocean.com:25060";
    private String driverClassName = "org.postgresql.Driver";

    // Método para criar um DataSource dinâmico
    public DataSource createDataSource(String tipo, String databaseName)
    {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(cloudUrl + "/" + databaseName);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }

    // Método para testar a conexão
    public String testarConexao(String databaseName) {
        try {
            DataSource dataSource = createDataSource("cloud", databaseName);
            Connection connection = dataSource.getConnection();
            connection.close();
            return "Conexão bem-sucedida ao banco de dados: " + databaseName;
        } catch (SQLException e) {
            System.err.println("Erro ao conectar ao banco de dados: " + e.getMessage());
            return "Erro ao conectar ao banco de dados: " + e.getMessage();

        }
    }


    // private String getUrl(String tipo, String databaseName) {
    //     if ("cloud".equalsIgnoreCase(tipo)) {
    //         return cloudUrl + "/" + databaseName; // Adiciona o nome do banco de dados
    //     } else if ("w5i".equalsIgnoreCase(tipo)) {
    //         return localUrl + "/" + databaseName; // Adiciona o nome do banco de dados
    //     } else {
    //         throw new IllegalArgumentException("Tipo de banco de dados inválido: " + tipo);
    //     }
    // }

    // private String getUsername(String tipo) {
    //     if ("cloud".equalsIgnoreCase(tipo)) {
    //         return usernameCloud;
    //     } else if ("w5i".equalsIgnoreCase(tipo)) {
    //         return usernameW5i;
    //     } else {
    //         throw new IllegalArgumentException("Tipo de banco de dados inválido: " + tipo);
    //     }
    // }

    // private String getPassword(String tipo) {
    //     if ("cloud".equalsIgnoreCase(tipo)) {
    //         return passwordCloud;
    //     } else if ("w5i".equalsIgnoreCase(tipo)) {
    //         return passwordW5i;
    //     } else {
    //         throw new IllegalArgumentException("Tipo de banco de dados inválido: " + tipo);
    //     }
    // }
}