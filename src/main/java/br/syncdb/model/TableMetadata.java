package br.syncdb.model;

public class TableMetadata {
    private final String estruturaTabela;
    private final String chavesEstrangeiras;

    public TableMetadata(String estruturaTabela, String chavesEstrangeiras) {
        this.estruturaTabela = estruturaTabela;
        this.chavesEstrangeiras = chavesEstrangeiras;
    }

    public String getEstruturaTabela() {
        return estruturaTabela;
    }

    public String getChavesEstrangeiras() {
        return chavesEstrangeiras;
    } 
}
