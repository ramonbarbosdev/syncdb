package br.syncdb.controller;

public enum TipoConexao 
{
    CLOUD("db.cloud"),
    LOCAL("db.local");

    private final String prefixo;

    TipoConexao(String prefixo) {
        this.prefixo = prefixo;
    }

    public String getPrefixo()
    {
        return prefixo;
    }
}
