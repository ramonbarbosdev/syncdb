package br.syncdb.config;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class TabelaCacheManager
{
    private static final Cache<String, Set<String>> cache = Caffeine.newBuilder()
    .maximumSize(100) // Máximo de 100 entradas no cache
    .expireAfterWrite(30, TimeUnit.MINUTES) // Expira após 30 minutos
    .recordStats() // Para monitoramento
    .build();

    public static void inserirCache(String chave, Set<String> tabelas) {
        cache.put(chave, tabelas);
    }

    public static Set<String> obterCache(String chave) {
        return cache.getIfPresent(chave);
    }

    public static void limparCache(String chave) {
        cache.invalidate(chave);
    }

    public static Map<String, Object> getEstatisticasCache() {
        return Map.of(
            "hitRate", cache.stats().hitRate(),
            "missCount", cache.stats().missCount(),
            "loadCount", cache.stats().loadCount()
        );
    }
}
