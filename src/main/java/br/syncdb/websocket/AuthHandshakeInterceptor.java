package br.syncdb.websocket;

import br.syncdb.security.JWTTokenAutenticacaoService;
import jakarta.servlet.http.HttpServletRequest;

import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.Map;

@Component
public class AuthHandshakeInterceptor implements HandshakeInterceptor {
    @Override
    public boolean beforeHandshake(
            ServerHttpRequest request,
            ServerHttpResponse response,
            WebSocketHandler wsHandler,
            Map<String, Object> attributes) throws Exception {
                
    
        // if (request instanceof ServletServerHttpRequest servletRequest) {

        //     String token = servletRequest.getServletRequest().getHeader("Authorization");
        //     System.out.println(token);

        //     if (token != null && token.startsWith("Bearer "))
        //     {
        //         token = token.substring(7); 
                
        //         System.out.println("Token recebido no WebSocket: " + token);
        //     }
        //     else
        //     {
        //         System.out.println("Token de autorização não encontrado ou formato inválido.");
        //         return false; 
        //     }
        // }
        return true;
    }
    

    @Override
    public void afterHandshake(
        ServerHttpRequest request,
        ServerHttpResponse response,
        WebSocketHandler wsHandler,
        Exception exception
    ) {
        // pós-handshake, se precisar
    }
}