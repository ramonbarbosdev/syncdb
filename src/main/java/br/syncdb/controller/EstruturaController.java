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
import br.syncdb.service.SincronizacaoService;

@RestController 
@RequestMapping(value = "/estrutura")
public class EstruturaController {
    
    @Autowired
	private SincronizacaoService sincronizacaoService;

	@Autowired
	private DatabaseService databaseService;
	
	@GetMapping(value = "/bases/", produces = "application/json")
	public ResponseEntity<?> obterEstruturas ( ) 
	{
		List<String> bases = databaseService.listarBases("w5i_tecnologia", TipoConexao.CLOUD);

		if(!bases.isEmpty())
		{
			return ResponseEntity.ok(bases);
		}

		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{\"error\": \"NÃO EXISTE BASES\"}");
	}

	@GetMapping(value = "/verificar/{base}", produces = "application/json")
	public ResponseEntity<?> verificarEstrutura ( @PathVariable (value = "base") String base ) 
	{
		Map<String, Object>  resultado = sincronizacaoService.verificarEstrutura(base,  null);

		if ((Boolean) resultado.get("success"))
		{
			return ResponseEntity.ok(resultado);
		}
		else
		{
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
							   .body(resultado);
		}

	}
	
    @GetMapping(value = "/{base}", produces = "application/json")
	public ResponseEntity<?> sincronizacaoEstruturaTotal ( @PathVariable (value = "base") String base ) 
	{
		Map<String, Object>  resultado = sincronizacaoService.sincronizarEstrutura(base,  null);

		if ((Boolean) resultado.get("success"))
		{
			return ResponseEntity.ok(resultado);
		}
		else
		{
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(resultado);
		}

	}
    @GetMapping(value = "/{base}/{tabela}", produces = "application/json")
	public ResponseEntity<?> sincronizacaoEstruturaIndividual ( @PathVariable (value = "base") String base, @PathVariable (value = "tabela") String tabela ) 
	{
	
		Map<String, Object> resultado = sincronizacaoService.sincronizarEstrutura(base,  tabela);

		if ((Boolean) resultado.get("success"))
		{
			return ResponseEntity.ok(resultado);
		}
		else
		{
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
							   .body(resultado);
		}


	}
   
}
