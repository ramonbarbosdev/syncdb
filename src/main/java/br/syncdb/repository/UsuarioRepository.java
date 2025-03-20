package br.syncdb.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import br.syncdb.model.Usuario;
import jakarta.transaction.Transactional;

@Repository
@Transactional
public interface  UsuarioRepository extends CrudRepository<Usuario, Long>  {

	//consultar usuario por login
	@Query("select u from Usuario u where u.login = ?1")
	Usuario findUserByLogin(String login);
	
	@org.springframework.transaction.annotation.Transactional
	@Modifying
	@Query(nativeQuery = true, value = "update usuario set token = ?1 where login = ?2")
	void atualizarTokenUser(String token, String login);
	
}
