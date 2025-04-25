package br.syncdb.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.syncdb.DTO.ConexaoDTO;
import br.syncdb.model.Conexao;
import br.syncdb.model.Usuario;
import br.syncdb.repository.UsuarioRepository;
import br.syncdb.security.JWTTokenAutenticacaoService;
import jakarta.servlet.http.HttpServletResponse;

@RestController 
@RequestMapping(value = "/auth")
public class AuthController {
    
    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private UsuarioRepository usuarioRepository;

    @Autowired
    private JWTTokenAutenticacaoService jwtTokenAutenticacaoService;

    @PostMapping(value = "/login", produces = "application/json")
    public ResponseEntity login(@RequestBody Map<String, Object> obj, HttpServletResponse response) 
    {
        String login = (String) obj.get("login"); 
        String senha  = (String) obj.get("senha"); 

        var usernamePassword = new UsernamePasswordAuthenticationToken(login, senha);
        var auth =  this.authenticationManager.authenticate(usernamePassword);

        try
        {
            String token = jwtTokenAutenticacaoService.addAuthentication(response, auth.getName());
            return ResponseEntity.ok().body(Map.of("Authorization", token));
        }
        catch (Exception e)
        {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body( Map.of(
                                                                            "error", "Usuário ou senha invalidos!",
                                                                            "code", 402
                                                                        ));
        }

    }

    @PostMapping(value = "/register", produces = "application/json")
    public ResponseEntity register(@RequestBody Map<String, Object> obj)
    {
        String login = (String) obj.get("login"); 
        String nome = (String) obj.get("nome"); 
        String senha = (String) obj.get("senha"); 
    
        if (usuarioRepository.findUserByLogin(login) != null)
        {
            return ResponseEntity.status(HttpStatus.CONFLICT).body( Map.of(
                                                                            "error", "Usuário já existe!",
                                                                            "code", 409
                                                                        ));
        }
    
        String senhaCriptografada = new BCryptPasswordEncoder().encode(senha);
    
        Usuario usuario = new Usuario();
        usuario.setLogin(login);
        usuario.setNome(nome);
        usuario.setSenha(senhaCriptografada);
    
        Usuario usuarioSalvo = usuarioRepository.save(usuario);
    
        return new ResponseEntity<Usuario>(usuarioSalvo, HttpStatus.CREATED);
    }
    
    
}
