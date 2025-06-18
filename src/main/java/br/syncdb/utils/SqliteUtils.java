package br.syncdb.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SqliteUtils {

    /**
     * Monta a URL JDBC para SQLite a partir de um caminho customizado ou, se vazio/nulo,
     * faz fallback baseado no SO do usuário.
     *
     * Retorna algo como "jdbc:sqlite:C:/Users/xxx/.../syncdb.sqlite".
     *
     * @param dbFilePath caminho absoluto ou relativo fornecido; pode ser null ou vazio para fallback
     * @return URL JDBC completa para SQLite
     */
    public static String montaJdbcUrl(String dbFilePath) {
        Path p;
        if (dbFilePath != null && !dbFilePath.trim().isEmpty()) {
            // Usa o caminho especificado
            p = Paths.get(dbFilePath).normalize().toAbsolutePath();
            System.out.println("[SqliteUtils] Usando caminho customizado para SQLite: " + p);
        } else {
            // Fallback baseado em SO
            String userHome = System.getProperty("user.home");
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("win")) {
                p = Paths.get(userHome, "AppData", "Roaming", "syncdb-desktop", "data", "syncdb.sqlite");
            } else if (os.contains("mac")) {
                p = Paths.get(userHome, "Library", "Application Support", "syncdb-desktop", "data", "syncdb.sqlite");
            } else {
                // Linux e outros Unix-like
                p = Paths.get(userHome, ".config", "syncdb-desktop", "data", "syncdb.sqlite");
            }
            p = p.normalize().toAbsolutePath();
            System.out.println("[SqliteUtils] Usando fallback para SQLite: " + p);
        }
        return "jdbc:sqlite:" + p.toString();
    }

    /**
     * Garante que o diretório pai do arquivo SQLite exista, criando-o se necessário.
     * Recebe a URL JDBC ou diretamente o caminho do arquivo?
     * Aqui, fornecemos o caminho do arquivo (sem prefixo "jdbc:sqlite:").
     *
     * @param dbFilePathString caminho completo do arquivo SQLite (sem prefixo "jdbc:sqlite:")
     */
    public static void garantirDiretorioPai(String dbFilePathString) {
        Path dbPath = Paths.get(dbFilePathString).normalize().toAbsolutePath();
        Path parentDir = dbPath.getParent();
        if (parentDir != null) {
            try {
                if (Files.notExists(parentDir)) {
                    Files.createDirectories(parentDir);
                    System.out.println("[SqliteUtils] Diretório criado: " + parentDir);
                }
            } catch (IOException e) {
                System.err.println("[SqliteUtils] Erro ao criar diretório pai: " + e.getMessage());
                throw new RuntimeException("Falha ao criar diretório para SQLite: " + parentDir, e);
            }
        }
    }

    /**
     * Dado uma URL JDBC do SQLite ("jdbc:sqlite:C:/..."), extrai o caminho do arquivo,
     * ou seja, remove o prefixo "jdbc:sqlite:".
     */
    public static String extrairCaminhoArquivo(String jdbcUrl) {
        if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:sqlite:")) {
            return jdbcUrl.substring("jdbc:sqlite:".length());
        }
        return jdbcUrl;
    }
}
