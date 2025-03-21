package br.syncdb.controller;

import java.util.ArrayList;
import java.util.List;
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
import br.syncdb.config.DataConfigGenerico;
import br.syncdb.model.Usuario;
import br.syncdb.repository.UsuarioRepository;
import br.syncdb.service.DatabaseService;

@RestController 
@RequestMapping(value = "/sincronizacao")
public class SincronizacaoController
{
	@Autowired
	private DatabaseService databaseService;

	
	
    // @GetMapping(value = "/base", produces = "application/json")
	// public ResponseEntity<List<?>> listaBase () 
	// {

	// 	List lista = databaseService.listDatabases();
	
	// 	return new ResponseEntity<>(lista, HttpStatus.OK);

	// }
	
    @GetMapping(value = "/base/{base}/{banco}", produces = "application/json")
	public ResponseEntity<?> listarTabelas ( @PathVariable (value = "base") String base ,  @PathVariable (value = "banco") String banco) 
	{
		
		List lista = databaseService.obterBanco(base, banco);

		if(lista.isEmpty())
		{
	        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("{\"error\": \"Base não encontrada\"}");
		}
	
		return new ResponseEntity<>(lista, HttpStatus.OK);

	}
   

}