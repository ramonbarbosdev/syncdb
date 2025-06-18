package br.syncdb.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import br.syncdb.utils.SqliteUtils;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Value("${spring.datasource.custom-path:}")
    private String customDbFilePath;

    @Bean
    public DataSource dataSource() {
        // 1. Monta a URL JDBC
        String jdbcUrl = SqliteUtils.montaJdbcUrl(customDbFilePath);

        // 2. Extrai caminho do arquivo e garante diretório
        String arquivoPath = SqliteUtils.extrairCaminhoArquivo(jdbcUrl);
        SqliteUtils.garantirDiretorioPai(arquivoPath);

        System.out.println("[DataSourceConfig] Conectando com SQLite via: " + jdbcUrl);
        return DataSourceBuilder.create()
                .driverClassName("org.sqlite.JDBC")
                .url(jdbcUrl)
                .build();
    }
}
