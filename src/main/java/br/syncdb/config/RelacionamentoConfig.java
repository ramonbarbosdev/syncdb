package br.syncdb.config;

import org.springframework.data.repository.CrudRepository;

public class RelacionamentoConfig {
    private CrudRepository repository;
    private String setter;
    private Class<?> entidadeRelacionada;

    public RelacionamentoConfig(CrudRepository repository, String setter, Class<?> entidadeRelacionada) {
        this.repository = repository;
        this.setter = setter;
        this.entidadeRelacionada = entidadeRelacionada;
    }

    public CrudRepository  getRepository() {
        return repository;
    }

    public String getSetter() {
        return setter;
    }

    public Class<?> getEntidadeRelacionada() {
        return entidadeRelacionada;
    }
}
