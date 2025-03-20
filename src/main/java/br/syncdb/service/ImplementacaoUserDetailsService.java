package br.syncdb.service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import br.syncdb.model.Usuario;
import br.syncdb.repository.UsuarioRepository;

@Service
public class ImplementacaoUserDetailsService implements UserDetailsService{

	@Autowired
	private UsuarioRepository usuarioRepository;
	
	@Override
	public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
		
		/*Consulta no banco o usuario*/
		
		Usuario usuario = usuarioRepository.findUserByLogin(username);
		
		if(usuario == null)
		{
			throw new UsernameNotFoundException("Usuario não encontrado!");
		}
		
		/*Retorna o login e senha do usuario com as autorizações*/
		return new User(usuario.getLogin(), usuario.getPassword(), usuario.getAuthorities());
	}

}
