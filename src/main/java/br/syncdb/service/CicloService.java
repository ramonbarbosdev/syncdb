package br.syncdb.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.springframework.stereotype.Service;

@Service
public class CicloService
{
 
    
    public void ordenacaoTopologica(String tabela, Map<String, Set<String>> dependencias,
    Set<String> visitadas, Set<String> emProcessamento,
    List<String> ordenadas, Set<Set<String>> ciclos)
{
    Optional<Set<String>> ciclo = encontrarCicloParaTabela(tabela, ciclos);

    if (ciclo.isPresent())
    {
        if (!visitadas.contains(tabela))
        {
            tratarTabelaComCiclo(ciclo.get(), dependencias, visitadas, ordenadas);
        }
        return;
    }

    // Processamento normal para tabelas sem ciclos
    if (emProcessamento.contains(tabela))
    {
        throw new IllegalStateException("Ciclo não tratado detectado: " + tabela);
    }

    if (visitadas.contains(tabela)) return;

    emProcessamento.add(tabela);

    for (String dependencia : dependencias.getOrDefault(tabela, Collections.emptySet()))
    {
        ordenacaoTopologica(dependencia, dependencias, visitadas, emProcessamento, ordenadas, ciclos);
    }

    emProcessamento.remove(tabela);
    visitadas.add(tabela);
    ordenadas.add(tabela);
}

    public void detectarCiclos(Map<String, Set<String>> dependencias, Set<Set<String>> ciclos)
    {
        for (String tabela : dependencias.keySet())
        {
            Set<String> caminhoAtual = new LinkedHashSet<>();
            detectarCicloRecursivo(tabela, dependencias, new HashSet<>(), caminhoAtual, ciclos);
        }
    }

     private void detectarCicloRecursivo(String atual, Map<String, Set<String>> dependencias,
                                  Set<String> visitadas, Set<String> caminhoAtual,
                                  Set<Set<String>> ciclos)
    {
        if (caminhoAtual.contains(atual))
        {
            // Encontrou um ciclo - adicionar ao conjunto de ciclos
            Set<String> ciclo = new LinkedHashSet<>();
            boolean dentroDoCiclo = false;
            
            for (String t : caminhoAtual) {
                if (t.equals(atual)) dentroDoCiclo = true;
                if (dentroDoCiclo) ciclo.add(t);
            }
            ciclo.add(atual);
            
            ciclos.add(ciclo);
            return;
        }

        if (visitadas.contains(atual)) return;

        visitadas.add(atual);
        caminhoAtual.add(atual);

        for (String vizinho : dependencias.getOrDefault(atual, Collections.emptySet())) {
            detectarCicloRecursivo(vizinho, dependencias, visitadas, caminhoAtual, ciclos);
        }

        caminhoAtual.remove(atual);
    }

    private Optional<Set<String>> encontrarCicloParaTabela(String tabela, Set<Set<String>> ciclos)
    {
        return ciclos.stream()
            .filter(ciclo -> ciclo.contains(tabela))
            .findFirst();
    }
    
    private void tratarTabelaComCiclo(Set<String> ciclo, Map<String, Set<String>> dependencias,
    Set<String> visitadas, List<String> ordenadas)
    {
        boolean cicloJaProcessado = ciclo.stream().anyMatch(visitadas::contains);

        if (cicloJaProcessado)
        {
            return;
        }

        // Escolher estratégia para lidar com o ciclo:
        // 1. Ordenar as tabelas do ciclo internamente
        List<String> cicloOrdenado = ordenarCicloInternamente(ciclo, dependencias);

        // 2. Adicionar todas as tabelas do ciclo à lista ordenada
        for (String tabela : cicloOrdenado)
        {
            if (!visitadas.contains(tabela))
            {
                visitadas.add(tabela);
                ordenadas.add(tabela);
            }
        }
    }

    private List<String> ordenarCicloInternamente(Set<String> ciclo, Map<String, Set<String>> dependencias)
    {
        // Estratégia simples: ordenar alfabeticamente
        List<String> ordenado = new ArrayList<>(ciclo);
        Collections.sort(ordenado);
        
        // Ou implementar lógica mais sofisticada baseada em:
        // - Tabelas com menos dependências externas primeiro
        // - Tabelas menores primeiro
        // - Outros critérios específicos do seu domínio
        
        return ordenado;
    }
}
