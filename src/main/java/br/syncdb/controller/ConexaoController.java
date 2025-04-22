package br.syncdb.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.syncdb.DTO.ConexaoDTO;

@RestController
@RequestMapping("/conexao")
public class ConexaoController
{
    @PostMapping(value = "/", produces = "application/json")
    public ResponseEntity<?> salvarConexao(@RequestBody ConexaoDTO conexaoDTO)
    {
        System.out.println("Cloud Host: " + conexaoDTO.getCloud().getDb_cloud_host());
        System.out.println("Local Host: " + conexaoDTO.getLocal().getDb_local_host());

       return ResponseEntity.status(HttpStatus.CREATED).body("{\"sucesso\": \"Conexao criada com sucesso!\"}");
    }
}
