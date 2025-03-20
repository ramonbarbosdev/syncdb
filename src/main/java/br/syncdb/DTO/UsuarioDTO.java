package br.syncdb.DTO;

import java.io.Serializable;

import br.syncdb.model.Usuario;



public class UsuarioDTO  implements Serializable{

	private String userLogin;
	private String userNome;
	private String userSenha;   


	public UsuarioDTO(Usuario usuario) 
	{
		this.userLogin = usuario.getLogin();
		this.userNome = usuario.getNome();
		this.userSenha = usuario.getSenha();

	}

	public String getUserLogin() {
		return userLogin;
	}	
	public void setUserLogin(String userLogin) {
		this.userLogin = userLogin;
	}

	public String getUserNome() {
		return userNome;
	}

	public void setUserNome(String userNome) {
		this.userNome = userNome;
	}

	public String getUserSenha() {
		return userSenha;
	}

	public void setUserSenha(String userSenha) {
		this.userSenha = userSenha;
	}
}
