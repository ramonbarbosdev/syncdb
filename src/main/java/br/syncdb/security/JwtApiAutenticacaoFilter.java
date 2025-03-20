package br.syncdb.security;

import java.io.IOException;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

//Filtro onde todas as requisicoes serão capturadas para autenticar
public class JwtApiAutenticacaoFilter  extends GenericFilterBean{

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		
	   // System.out.println("Filtro de autenticação iniciado.");
		 // Verifica se já existe um usuário autenticado
        if (SecurityContextHolder.getContext().getAuthentication() == null)
        {
            
            // Se não houver autenticação, tenta autenticar com JWT
            Authentication authentication = new JWTTokenAutenticacaoService()
                    .getAuthentication((HttpServletRequest) request, (HttpServletResponse) response);

            // Define a autenticação no contexto do Spring Security, se for válida
            if (authentication != null) {
                SecurityContextHolder.getContext().setAuthentication(authentication);
            }
        }

        // Continua o fluxo normal
        chain.doFilter(request, response);
	}

}
