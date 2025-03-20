package  br.syncdb.model;

import java.time.LocalDate;

import javax.xml.crypto.Data;

import org.springframework.security.core.GrantedAuthority;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotBlank;

@Entity
@Table(name = "registro_base")
@SequenceGenerator(name = "seq_registro_base", sequenceName = "seq_registro_base", allocationSize = 1, initialValue = 1 )
public class RegistroBase 
{
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_registro_base")
	private Long id_registrobase;
	
	@NotBlank(message = "A Data é obrigatorio!")
	@Column( nullable = false)
	private LocalDate dt_registrobase; 
	
	@NotBlank(message = "A Status é obrigatorio!")
	@Column( nullable = false)
	private String nm_basedados ; 

	@Column( nullable = false)
	private String nm_tabela; 

	@Column( nullable = false)
	private String ds_origem ; 

	public Long getId_registrobase()
	{
		return id_registrobase; 
	}

	public void setId_registrobase()
	{
		this.id_registrobase = id_registrobase;
	}
	public LocalDate getDt_registrobase()
	{
		return dt_registrobase; 
	}

	public void setDt_registrobase()
	{
		this.dt_registrobase = dt_registrobase;
	}

	public String getNm_basedados()
	{
		return nm_basedados ; 
	}

	public void setNm_basedados()
	{
		this.nm_basedados  = nm_basedados ;
	}

	public String getNm_tabela()
	{
		return nm_tabela; 
	}

	public void setNm_tabela()
	{
		this.nm_tabela = nm_tabela;
	}

	public String getDs_origem()
	{
		return ds_origem;
	}
	public void setDs_origem()
	{
		this.ds_origem = ds_origem;
	}


}
