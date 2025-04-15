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
import br.syncdb.model.Usuario;
import br.syncdb.repository.UsuarioRepository;
import br.syncdb.service.DatabaseService;
import br.syncdb.service.SincronizacaoService;

@RestController 
@RequestMapping(value = "/dados")
public class DadosController
{

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
	public ResponseEntity<?> verificarDados ( @PathVariable (value = "base") String base ) 
	{
		Map<String, Object>  resultado = sincronizacaoService.verificarDados(base,  null);

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
	
	
    @GetMapping(value = "/bases/{base}/{tabela}", produces = "application/json")
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
    @GetMapping(value = "/{base}", produces = "application/json")
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
	

}