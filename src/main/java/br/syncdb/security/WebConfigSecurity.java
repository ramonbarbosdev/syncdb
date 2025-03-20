package br.syncdb.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

import br.syncdb.service.ImplementacaoUserDetailsService;

@Configuration
@EnableWebSecurity
public class WebConfigSecurity {

    @Autowired
    private ImplementacaoUserDetailsService implementacaoUserDetailsService;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, AuthenticationManager authenticationManager) throws Exception {
      
    	http
            .csrf(csrf -> csrf.disable())  // Desabilita CSRF (necessário para APIs stateless)
            .authorizeHttpRequests(auth -> auth
            		.requestMatchers(HttpMethod.OPTIONS,"/**", "/index", "/login").permitAll()   // Permite acesso público
            	    .anyRequest().authenticated() 
            )
            .logout(logout -> logout
                .logoutRequestMatcher(new AntPathRequestMatcher("/logout"))  // Define o caminho para logout
                .logoutSuccessUrl("/index")  // Redireciona para a página inicial após logout
                .permitAll()
            )
            .addFilterBefore(new JwtApiAutenticacaoFilter(), UsernamePasswordAuthenticationFilter.class)
            .addFilterBefore(new JWTLoginFilter("/login", authenticationManager), UsernamePasswordAuthenticationFilter.class)
            .httpBasic();  // Permite autenticação básica (se necessário)

        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration authenticationConfiguration) throws Exception {
        return authenticationConfiguration.getAuthenticationManager();
    }
}