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
	private SincronizacaoService sincronizacaoService;

	@Autowired
	private DatabaseService databaseService;

	@Autowired
	private EstruturaService estruturaService;
	
	@GetMapping(value = "/bases/", produces = "application/json")
	public ResponseEntity<?> obterBase ( ) 
	{
		List<String> bases = databaseService.listarBases("w5i_tecnologia", TipoConexao.CLOUD);

		if(!bases.isEmpty())
		{
			return ResponseEntity.ok(bases);
		}

		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{\"erro\": \"Base informada não existe\"}");
	}
	@GetMapping(value = "/base/esquema/{base}", produces = "application/json")
	public ResponseEntity<?> obterSchemaTabelaBase ( @PathVariable (value = "base") String base   ) 
	{
		List<String> esquema = databaseService.obterSchema(base, TipoConexao.CLOUD);

		if(!esquema.isEmpty()) return ResponseEntity.ok(esquema);

		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{\"erro\": \"Esquema informada não existe\"}");
	}
	@GetMapping(value = "/base/tabela/{base}/{esquema}", produces = "application/json")
	public ResponseEntity<?> obterTabelaBase ( @PathVariable (value = "base") String base, @PathVariable (value = "esquema") String esquema     ) 
	{
		List<String> tabelas = databaseService.obterBanco(base,esquema, TipoConexao.CLOUD);

		if(!tabelas.isEmpty()) return ResponseEntity.ok(tabelas);

		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{\"erro\": \"Tabela informada não existe\"}");
	}

	@GetMapping(value = "/verificar/{base}/{tabela}", produces = "application/json")
	public ResponseEntity<?> verificarEstruturaTabela ( @PathVariable (value = "base") String base , @PathVariable (value = "tabela") String tabela) 
	{
		Map<String, Object>  resultado = estruturaService.verificarEstrutura(base,  tabela);

		if ((Boolean) resultado.get("sucesso"))
		{
			return ResponseEntity.ok(resultado);
		}
		else
		{
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
							   .body(resultado);
		}

	}

	@GetMapping(value = "/verificar/{base}/", produces = "application/json")
	public ResponseEntity<?> verificarEstrutura ( @PathVariable (value = "base") String base ) 
	{
		Map<String, Object>  resultado = estruturaService.verificarEstrutura(base,  null);

		if ((Boolean) resultado.get("sucesso"))
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
	public ResponseEntity<?> sincronizacao ( @PathVariable (value = "base") String base ) 
	{
		Map<String, Object>  resultado = estruturaService.sincronizarEstrutura(base);

		if ((Boolean) resultado.get("sucesso"))
		{
			return ResponseEntity.ok(resultado);
		}
		else
		{
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(resultado);
		}

	}
  
   
}
