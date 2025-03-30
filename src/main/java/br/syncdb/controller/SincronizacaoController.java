package br.syncdb.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import br.syncdb.DTO.UsuarioDTO;
import br.syncdb.config.CacheManager;
import br.syncdb.model.Usuario;
import br.syncdb.repository.UsuarioRepository;
import br.syncdb.service.DatabaseService;
import br.syncdb.service.SincronizacaoService;

@RestController 
@RequestMapping(value = "/sincronizacao")
public class SincronizacaoController
{


	@Autowired
	private SincronizacaoService sincronizacaoService;
	

	
    @GetMapping(value = "/estrutura/{base}", produces = "application/json")
	public ResponseEntity<?> sincronizacaoEstruturaTotal ( @PathVariable (value = "base") String base ) 
	{
	
		Map<String, Object>  resultado = sincronizacaoService.sincronizarEstrutura(base,  null);

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
    @GetMapping(value = "/estrutura/{base}/{tabela}", produces = "application/json")
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
   
    @GetMapping(value = "/dados/{base}/{tabela}", produces = "application/json")
	public ResponseEntity<?> sincronizacaoDadosIndividual ( @PathVariable (value = "base") String base, @PathVariable (value = "tabela") String tabela ) 
	{
	
		Map<String, Object> resultado = sincronizacaoService.sincronizarDados(base,  tabela, false);

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
    @GetMapping(value = "/dados/{base}", produces = "application/json")
	public ResponseEntity<?> sincronizacaoDadosTotal ( @PathVariable (value = "base") String base ) 
	{
	
		Map<String, Object> resultado = sincronizacaoService.sincronizarDados(base,  null, false);

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
	

	// @GetMapping("/cache/stats")
	// public Map<String, Object> getCacheStats()
	// {
	// 	return TabelaCache.getEstatisticasCache();
	// }
   

}