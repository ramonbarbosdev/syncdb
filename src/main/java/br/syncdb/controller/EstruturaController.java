package br.syncdb.controller;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.syncdb.component.ProcessoManager;
import br.syncdb.service.DatabaseService;
import br.syncdb.service.EstruturaService;
import br.syncdb.service.SincronizacaoService;

@RestController 
@RequestMapping(value = "/estrutura")
public class EstruturaController {
    
	@Autowired
	private EstruturaService estruturaService;

	@Autowired
	private ProcessoManager processoManager;

	@GetMapping(value = "/verificar/{base}/{esquema}/{tabela}", produces = "application/json")
	public ResponseEntity<?> verificarEstruturaTabela ( @PathVariable (value = "base") String base , @PathVariable (value = "esquema") String esquema, @PathVariable (value = "tabela") String tabela) throws InterruptedException 
	{
		AtomicReference<Map<String, Object>> resultadoRef = new AtomicReference<>(new LinkedHashMap<>());

		processoManager.iniciarProcesso(() ->
		{
			Map<String, Object> resultado = estruturaService.verificarEstrutura(base, esquema, tabela);
			resultadoRef.set(resultado);
		});

		int tentativas = 0;
		int maxTentativas = 300; 
		while (processoManager.isExecutando() && tentativas++ < maxTentativas)
		{
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}

		Map<String, Object> resultado = resultadoRef.get();

		if (Boolean.TRUE.equals(resultado.get("sucesso"))) return new ResponseEntity<>(resultado, HttpStatus.OK);
				
		return new ResponseEntity<>(resultado, HttpStatus.NOT_FOUND);
		
	}

	@GetMapping(value = "/verificar/{base}/{esquema}", produces = "application/json")
	public ResponseEntity<?> verificarEstrutura ( @PathVariable (value = "base") String base, @PathVariable (value = "esquema") String esquema ) throws InterruptedException 
	{
		AtomicReference<Map<String, Object>> resultadoRef = new AtomicReference<>(new LinkedHashMap<>());

		processoManager.iniciarProcesso(() ->
		{
			Map<String, Object> resultado = estruturaService.verificarEstrutura(base, esquema, null);
			resultadoRef.set(resultado);
		});

		int tentativas = 0;
		int maxTentativas = 300; 
		while (processoManager.isExecutando() && tentativas++ < maxTentativas)
		{
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}

		Map<String, Object> resultado = resultadoRef.get();

		if (Boolean.TRUE.equals(resultado.get("sucesso"))) return new ResponseEntity<>(resultado, HttpStatus.OK);
				
		return new ResponseEntity<>(resultado, HttpStatus.NOT_FOUND);

	}

	@GetMapping(value = "/cancelar", produces = "application/json")
    public ResponseEntity<Void> cancelar()
	{
        processoManager.cancelarProcesso();
        return ResponseEntity.ok().build();
    }
	
    @GetMapping(value = "/{base}", produces = "application/json")
	public ResponseEntity<?> sincronizacao ( @PathVariable (value = "base") String base ) 
	{
		Map<String, Object>  resultado = estruturaService.sincronizarEstrutura(base);

		if ((Boolean) resultado.get("sucesso"))
		{
			return new ResponseEntity<Map<String, Object>>(resultado, HttpStatus.OK);
		}
		else
		{
			return new ResponseEntity<Map<String, Object>>(resultado, HttpStatus.NOT_FOUND);
		}

	}
  
   
}
