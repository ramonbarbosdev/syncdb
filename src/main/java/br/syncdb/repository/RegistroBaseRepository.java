package br.syncdb.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import br.syncdb.model.RegistroBase;
import br.syncdb.model.Sincronizacao;
import jakarta.transaction.Transactional;

@Repository
@Transactional
public interface  RegistroBaseRepository extends CrudRepository<RegistroBase, Long>  {

	
	
}
