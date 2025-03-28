package br.syncdb.config;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CacheManager
{
    private Cache<String, Map<String, Object>> cache;

    public CacheManager() {
        // Configura o cache com expiração de 10 minutos e um limite de 100 entradas
        this.cache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .maximumSize(100)
                .build();
    }

    // Método para obter dados do cache ou carregar se não estiver presente
    public Map<String, Object> getOrLoad(String key, DataLoader dataLoader) {
        // Tenta obter os dados do cache
        Map<String, Object> data = cache.getIfPresent(key);
        if (data == null) {
            // Se não estiver no cache, carrega os dados
            data = dataLoader.loadData();
            // Armazena os dados no cache
            cache.put(key, data);
        }
        return data;
    }

    // Interface para carregar dados
    public interface DataLoader {
        Map<String, Object> loadData();
    }
}
