package br.syncdb.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.github.benmanes.caffeine.cache.Cache;

@Service
public class EstruturaCacheService
{
    @Autowired
    private Cache<String, Object> estruturaCache;

    public void salvarCache(String chave, Object valor)
    {
        estruturaCache.put(chave, valor);
    }

    public <T> T buscarCache(String chave, Class<T> clazz)
    {
        Object valor = estruturaCache.getIfPresent(chave);
        if (clazz.isInstance(valor)) {
            return clazz.cast(valor);
        }
        return null;
    }

    public void removerCache(String chave) 
    {
        estruturaCache.invalidate(chave);
    }

    public void limparTudo()
    {
        estruturaCache.invalidateAll();
    }
}
