package br.syncdb.model;

public class TabelaDetalhe
{
    private String tabela;
    private String acao;
    private int linhaInseridas;
    private int linhaAtualizadas;

    public void setTabela(String tabela)
    {
        this.tabela = tabela;
    }
    public String getTabela()
    {
        return tabela;
    }
    public void setAcao(String acao)
    {
        this.acao = acao;
    }
    public String getAcao()
    {
        return acao;
    }
    public void setLinhaInseridas(int linhaInseridas)
    {
        this.linhaInseridas = linhaInseridas;
    }
    public int getLinhaInseridas()
    {
        return linhaInseridas;
    }
    public void setLinhaAtualizadas(int linhaAtualizadas)
    {
        this.linhaAtualizadas = linhaAtualizadas;
    }
    public int getLinhaAtualizadas()
    {
        return linhaAtualizadas;
    }
    
}
