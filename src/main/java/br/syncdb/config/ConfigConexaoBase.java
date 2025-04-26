package br.syncdb.config;

import io.github.cdimascio.dotenv.Dotenv;

public class ConfigConexaoBase
{
    private static final Dotenv dotenv = Dotenv.configure().filename(".env").load();

    public static String getLocalDbUrl() {
        return dotenv.get("DATABASE_LOCAL_URL");
    }

    public static String getLocalDbUsername() {
        return dotenv.get("DATABASE_LOCAL_USER");
    }

    public static String getLocalDbPassword() {
        return dotenv.get("DATABASE_LOCAL_PASS");
    }

    public static String getCloudDbUrl() {
        return dotenv.get("DATABASE_CLOUD_URL");
    }

    public static String getCloudDbUsername() {
        return dotenv.get("DATABASE_CLOUD_USER");
    }

    public static String getCloudDbPassword() {
        return dotenv.get("DATABASE_CLOUD_PASS");
    }
}
