package br.syncdb.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
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

//@CrossOrigin(origins = "*")
@RestController /*ARQUITETURA REST*/
@RequestMapping(value = "/usuario")
public class UsuarioController {
	
	@Autowired /*se fosse CDI seria @inject*/
	private UsuarioRepository usuarioRepository;
	
	/*SERVIÇO RESTFULL*/
	
	@GetMapping(value = "/{id}", produces = "application/json")
	@CacheEvict(value = "cacheuser", allEntries = true)
	@CachePut("cacheuser")
	public ResponseEntity<UsuarioDTO> init(@PathVariable (value="id") Long id )
	{
		
		Optional<Usuario> usuario =  usuarioRepository.findById(id);
		
		return new ResponseEntity<UsuarioDTO>(new UsuarioDTO(usuario.get()), HttpStatus.OK);
	}

	@GetMapping(value = "/", produces = "application/json")
	@CacheEvict(value = "cacheusuario", allEntries = true) //remover cache nao utilizado
	@CachePut("cacheusuario") //atualizar cache
	public ResponseEntity<List<?>> usuario () throws InterruptedException
	{
		 List<Usuario> usuarios = (List<Usuario>) usuarioRepository.findAll(); // Consulta todos os usuários
    
		// Mapeia cada objeto Usuario para UsuarioDTO
		List<UsuarioDTO> usuariosDTO = usuarios.stream()
				.map(usuario -> new UsuarioDTO(usuario)) // Usando o construtor para mapear
				.collect(Collectors.toList()); // Coleta todos os DTOs em uma lista
		
		return new ResponseEntity<>(usuariosDTO, HttpStatus.OK);
	}
	
	@PostMapping(value="/" , produces = "application/json")
	public ResponseEntity<Usuario> cadastrar(@RequestBody Usuario usuario)
	{
		
		
		String senhacriptografada = new BCryptPasswordEncoder().encode(usuario.getSenha());
		usuario.setSenha(senhacriptografada);
		Usuario usuarioSalvo = usuarioRepository.save(usuario);
		
		return new ResponseEntity<Usuario>(usuarioSalvo, HttpStatus.OK);
	}
	
	@PutMapping(value="/" , produces = "application/json")
	public ResponseEntity<?> atualizar(@RequestBody Usuario usuario)
	{
		
		
	
		Usuario userTemporario = usuarioRepository.findUserByLogin(usuario.getLogin());
		
		if (userTemporario == null)
		{
	        return ResponseEntity.status(HttpStatus.NOT_FOUND).body("{\"error\": \"Usuário não encontrado.\"}");
	    }
		
	    if (usuario.getSenha() == null || usuario.getSenha().trim().isEmpty())
	    {
	        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("{\"error\": \"Senha inválida. Não pode ser vazia.\"}");
	    }
		
		if (!userTemporario.getSenha().equals(usuario.getSenha()))
		{
			String senhacriptografada = new BCryptPasswordEncoder().encode(usuario.getSenha());
			usuario.setSenha(senhacriptografada);
		}
		
		Usuario usuarioSalvo = usuarioRepository.save(usuario);
		
		return new ResponseEntity<Usuario>(usuarioSalvo, HttpStatus.OK);
	}
	
	@DeleteMapping(value = "/{id}", produces = "application/text" )
	public String delete (@PathVariable("id") Long id)
	{
		usuarioRepository.deleteById(id);
		
		return "ok";
	}
	
	
	

}