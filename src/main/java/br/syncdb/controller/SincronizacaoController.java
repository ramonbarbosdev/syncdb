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

@RestController 
@RequestMapping(value = "/sincronizacao")
public class SincronizacaoController
{
    @Autowired
	private DatabaseService databaseService;

    @GetMapping(value = "/bases/", produces = "application/json")
	public ResponseEntity<?> obterEstruturas ( ) 
	{
		List<String> bases = databaseService.listarBases("w5i_tecnologia", TipoConexao.CLOUD);

		if(!bases.isEmpty()) return new ResponseEntity<List<String>>(bases, HttpStatus.OK);

		return new ResponseEntity<List<String>>(bases, HttpStatus.NOT_FOUND);
	}

	@GetMapping(value = "/base/esquema/{base}", produces = "application/json")
	public ResponseEntity<?> obterSchemaTabelaBase ( @PathVariable (value = "base") String base   ) 
	{
		List<String> esquema = databaseService.obterSchema(base, null,TipoConexao.CLOUD);

		if(!esquema.isEmpty()) 	return new ResponseEntity<List<String>>(esquema, HttpStatus.OK);

		return new ResponseEntity<List<String>>(esquema, HttpStatus.NOT_FOUND);
	}
	@GetMapping(value = "/base/tabela/{base}/{esquema}", produces = "application/json")
	public ResponseEntity<?> obterTabelaBase ( @PathVariable (value = "base") String base, @PathVariable (value = "esquema") String esquema     ) 
	{
		List<String> tabelas = databaseService.obterBanco(base,esquema, TipoConexao.CLOUD);

		if(!tabelas.isEmpty()) return new ResponseEntity<List<String>>(tabelas, HttpStatus.OK);

		return new ResponseEntity<List<String>>(tabelas, HttpStatus.NOT_FOUND);
	}

    @GetMapping(value = "/verificaesquema/{base}/{esquema}", produces = "application/json")
	public ResponseEntity<?> verificarExistenciaEsquema ( @PathVariable (value = "base") String base, @PathVariable (value = "esquema") String esquema     ) 
	{
		List<String> esquemas = databaseService.obterSchemaUnico(base, esquema,TipoConexao.LOCAL);

		if(!esquemas.isEmpty())  return new ResponseEntity<List<String>>(esquemas, HttpStatus.OK);

		return new ResponseEntity<List<String>>(esquemas, HttpStatus.NOT_FOUND);
	}

	

}
