package br.syncdb.websocket;

import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.TextMessage;


public class StatusWebSocketHandler implements WebSocketHandler {

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        System.out.println("Conexão WebSocket estabelecida.");
        session.sendMessage(new TextMessage("Conexão com o servidor WebSocket estabelecida com sucesso!"));
    }

    @Override
    public void handleMessage(WebSocketSession session, org.springframework.web.socket.WebSocketMessage<?> message) throws Exception {
        // Aqui você pode lidar com as mensagens recebidas
        String receivedMessage = (String) message.getPayload();
        System.out.println("Mensagem recebida: " + receivedMessage);
        // Envie uma resposta
        session.sendMessage(new TextMessage("Mensagem recebida: " + receivedMessage));
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        System.out.println("Erro no WebSocket: " + exception.getMessage());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, org.springframework.web.socket.CloseStatus closeStatus) throws Exception {
        System.out.println("Conexão WebSocket fechada. Status: " + closeStatus);
    }

    @Override
    public boolean supportsPartialMessages() {
        return false;
    }
}
