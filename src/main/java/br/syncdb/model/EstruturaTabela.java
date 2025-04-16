package br.syncdb.model;

public class EstruturaTabela
{
    private String tabela;
    private String acao;
    private String erro;
    private String querys;

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
    public void setErro(String erro)
    {
        this.erro = erro;
    }
    public String getErro()
    {
        return erro;
    }
    public void setQuerys(String querys)
    {
        this.querys = querys;
    }
    public String getQuerys()
    {
        return querys;
    }
    
}
