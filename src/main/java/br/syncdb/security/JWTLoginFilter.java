package br.syncdb.security;

import java.io.IOException;import java.time.temporal.ValueRange;

import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AbstractAuthenticationProcessingFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.util.AntPathMatcher;
import org.yaml.snakeyaml.tokens.ValueToken;

import com.fasterxml.jackson.databind.ObjectMapper;

import br.syncdb.model.Usuario;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class JWTLoginFilter extends AbstractAuthenticationProcessingFilter{

	//configurando gerenciador de autenticacao
	protected JWTLoginFilter(String url, AuthenticationManager authenticationManager) {
		
		//Obriga a autenticar a url
		super(new AntPathRequestMatcher(url));
		
		//gerenciador de authenticacao
		setAuthenticationManager(authenticationManager);
	
	}

	
	//retorna o susuario ao processar a autenticacao
	@Override
	public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response)
			throws AuthenticationException, IOException, ServletException {
		
		
		//pegando o token para validar
		
		Usuario user = new ObjectMapper().readValue(request.getInputStream(), Usuario.class);
		
		//Retorna o usuario login, senha e acessos
		return getAuthenticationManager().authenticate(new UsernamePasswordAuthenticationToken(user.getLogin(), user.getPassword()));
	}
	
	@Override
	protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response, FilterChain chain,
			Authentication authResult) throws IOException, ServletException {

		try
		{
			new JWTTokenAutenticacaoService().addAuthentication(response,authResult.getName());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
