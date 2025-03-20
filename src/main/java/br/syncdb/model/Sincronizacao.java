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
@Table(name = "sincronizacao")
@SequenceGenerator(name = "seq_sincronizacao", sequenceName = "seq_sincronizacao", allocationSize = 1, initialValue = 1 )
public class Sincronizacao 
{
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "seq_sincronizacao")
	private Long id_sincronizacao;
	
	@NotBlank(message = "A Data é obrigatorio!")
	@Column( nullable = false)
	private LocalDate dt_sincronizacao; 
	
	@NotBlank(message = "A Status é obrigatorio!")
	@Column( nullable = false)
	private String tp_status; 

	@Column( nullable = true)
	private String ds_mensagem; 

	public Long getId_sincronizacao()
	{
		return id_sincronizacao; 
	}

	public void setId_sincronizacao()
	{
		this.id_sincronizacao = id_sincronizacao;
	}
	public LocalDate getDt_sincronizacao()
	{
		return dt_sincronizacao; 
	}

	public void setDt_sincronizacao()
	{
		this.dt_sincronizacao = dt_sincronizacao;
	}

	public String getTp_status()
	{
		return tp_status; 
	}

	public void setTp_status()
	{
		this.tp_status = tp_status;
	}

	public String getDs_mensagem()
	{
		return ds_mensagem; 
	}

	public void setDs_mensagem()
	{
		this.ds_mensagem = ds_mensagem;
	}


}
