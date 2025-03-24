package br.syncdb.model;

import java.util.Objects;
public class Coluna {
    private String nome;
    private String tipo;
    private boolean nullable;
    private String defaultValor;

    // Getters e Setters
    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValor() {
        return defaultValor;
    }

    public void setDefaultValor(String defaultValor) {
        this.defaultValor = defaultValor;
    }
}
