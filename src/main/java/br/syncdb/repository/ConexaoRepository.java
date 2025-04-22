package br.syncdb.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import br.syncdb.model.Conexao;
import br.syncdb.model.Usuario;
import jakarta.transaction.Transactional;

@Repository
@Transactional
public interface  ConexaoRepository extends CrudRepository<Conexao, Long>  {

	@Query("select c from Conexao c  order by  1 asc limit 1")
	Conexao buscarConexaoPorOrdem();
}
