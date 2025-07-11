package br.syncdb.security;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

import br.syncdb.utils.CriptoUtils;

public class GerarConfig {
    
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Digite o segredo (chave): ");
        String segredo = scanner.nextLine();

        String json = """
            {
              "user": "doadmin",
              "password": "AVNS_ThcFV7CzqE1EzYP7W8z",
              "host": "db-postgresql-nyc3-32073-do-user-9424476-0.b.db.ondigitalocean.com",
              "port": "25060"
            }
            """;

        byte[] chave = CriptoUtils.gerarChave256(segredo);
        String criptografado = CriptoUtils.criptografar(json, chave);

        Files.writeString(Path.of("config.enc"), criptografado);
        System.out.println("Arquivo 'config.enc' gerado com sucesso.");
    }
}
