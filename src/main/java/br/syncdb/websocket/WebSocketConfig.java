package br.syncdb.websocket;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import br.syncdb.websocket.StatusWebSocketHandler;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(webSocketHandler(), "/syncdb-stocks")
                .setAllowedOrigins("http://localhost:4200")  // Permitir conexões do Angular
                .withSockJS();  // Usar SockJS para fallback em caso de falha do WebSocket
    }

    @Bean
    public WebSocketHandler webSocketHandler() {
        return new StatusWebSocketHandler(); // Seu WebSocket handler
    }
}
