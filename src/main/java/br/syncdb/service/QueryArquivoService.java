package br.syncdb.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Service;

@Service
public class QueryArquivoService
{
    
    public void salvarQueryEmArquivo(File diretorio, String nomeArquivo, String query)
    {
        try (FileWriter writer = new FileWriter(new File(diretorio, nomeArquivo), true))
        {
            writer.write(query + ";\n\n");
        }
        catch (IOException e)
        {
            System.err.println("Erro ao salvar query no arquivo: " + e.getMessage());
        }
    }

    public void salvarQueriesEmArquivo(File diretorio, String nomeArquivo, List<String> queries)
    {
        try (FileWriter writer = new FileWriter(new File(diretorio, nomeArquivo)))
        {
            for (String query : queries)
            {
                writer.write(query + ";\n\n");
            }
        }
        catch (IOException e)
        {
            System.err.println("Erro ao salvar queries no arquivo: " + e.getMessage());
        }
    }

    public void salvarQueriesAgrupadas(File diretorio, Map<String, List<String>> queries)
    {
        queries.forEach((tipo, listaQueries) ->
        {
            if (!listaQueries.isEmpty())
            {
                String nomeArquivo = tipo.toLowerCase().replace(" ", "_") + ".sql";
                salvarQueriesEmArquivo(diretorio, nomeArquivo, listaQueries);
            }
        });
    }

 

}
