package br.syncdb.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.syncdb.service.DatabaseService;
import br.syncdb.service.EstruturaService;
import br.syncdb.service.SincronizacaoService;

@RestController 
@RequestMapping(value = "/estrutura")
public class EstruturaController {
    
	@Autowired
	private EstruturaService estruturaService;

	@GetMapping(value = "/verificar/{base}/{esquema}/{tabela}", produces = "application/json")
	public ResponseEntity<?> verificarEstruturaTabela ( @PathVariable (value = "base") String base , @PathVariable (value = "esquema") String esquema, @PathVariable (value = "tabela") String tabela) 
	{
		Map<String, Object>  resultado = estruturaService.verificarEstrutura(base,esquema,  tabela);

		if ((Boolean) resultado.get("sucesso"))
		{
			return new ResponseEntity<Map<String, Object>>(resultado, HttpStatus.OK);
		}
		else
		{
			return new ResponseEntity<Map<String, Object>>(resultado, HttpStatus.NOT_FOUND);
		}

	}

	@GetMapping(value = "/verificar/{base}/{esquema}", produces = "application/json")
	public ResponseEntity<?> verificarEstrutura ( @PathVariable (value = "base") String base, @PathVariable (value = "esquema") String esquema ) 
	{
		Map<String, Object>  resultado = estruturaService.verificarEstrutura(base, esquema,  null);

		if ((Boolean) resultado.get("sucesso"))
		{
			return new ResponseEntity<Map<String, Object>>(resultado, HttpStatus.OK);
		}
		else
		{
			return new ResponseEntity<Map<String, Object>>(resultado, HttpStatus.NOT_FOUND);
		}

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
