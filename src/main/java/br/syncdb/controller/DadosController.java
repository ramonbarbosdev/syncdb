package br.syncdb.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
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
import br.syncdb.component.ProcessoManager;
import br.syncdb.model.Conexao;
import br.syncdb.model.Usuario;
import br.syncdb.repository.UsuarioRepository;
import br.syncdb.service.DadosService;
import br.syncdb.service.DatabaseService;
import br.syncdb.service.SincronizacaoService;

@RestController 
@RequestMapping(value = "/dados")
public class DadosController
{

    @Autowired
	private SincronizacaoService sincronizacaoService;


	@Autowired
	private DadosService dadosService;

	@Autowired
	private ProcessoManager processoManager;

	@GetMapping(value = "/verificar/{base}/{esquema}", produces = "application/json")
	public ResponseEntity<?> verificarDados (@PathVariable (value = "base") String base, @PathVariable (value = "esquema") String esquema ) throws InterruptedException 
	{
		AtomicReference<Map<String, Object>> resultadoRef = new AtomicReference<>(new LinkedHashMap<>());

		processoManager.iniciarProcesso(() ->
		{
			Map<String, Object>  resultado = dadosService.verificarDados(base,  null);
			resultadoRef.set(resultado);
		});

		while (processoManager.isExecutando()) {
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
	 
	@GetMapping(value = "/verificar/{base}/{esquema}/{tabela}", produces = "application/json")
	public ResponseEntity<?> verificarDadosTabela (@PathVariable (value = "base") String base, @PathVariable (value = "esquema") String esquema,  @PathVariable (value = "tabela") String tabela ) throws InterruptedException  
	{
		AtomicReference<Map<String, Object>> resultadoRef = new AtomicReference<>(new LinkedHashMap<>());

		processoManager.iniciarProcesso(() ->
		{
			Map<String, Object>  resultado = dadosService.verificarDados(base,  tabela);
			resultadoRef.set(resultado);
		});

		while (processoManager.isExecutando()) {
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
		Map<String, Object> resultado = dadosService.sincronizarDados(base,  null, false);

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