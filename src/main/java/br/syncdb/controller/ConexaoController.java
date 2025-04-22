package br.syncdb.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.syncdb.DTO.ConexaoDTO;
import br.syncdb.model.Conexao;
import br.syncdb.repository.ConexaoRepository;

@RestController
@RequestMapping("/conexao")
public class ConexaoController
{
    @Autowired
    private ConexaoRepository repository;

    @PostMapping(value = "/", produces = "application/json")
    public ResponseEntity<?> salvarConexao(@RequestBody ConexaoDTO conexaoDTO)
    {
        System.out.println("Cloud Host: " + conexaoDTO.getCloud().getDb_cloud_host());
        System.out.println("Local Host: " + conexaoDTO.getLocal().getDb_local_host());

        Conexao conexaoModel = new Conexao();

        conexaoModel.setDb_cloud_host(conexaoDTO.getCloud().getDb_cloud_host());
        conexaoModel.setDb_cloud_port(conexaoDTO.getCloud().getDb_cloud_port());
        conexaoModel.setDb_cloud_user(conexaoDTO.getCloud().getDb_cloud_user());
        conexaoModel.setDb_cloud_password(conexaoDTO.getCloud().getDb_cloud_password());
        
        repository.save(conexaoModel);

       return ResponseEntity.status(HttpStatus.CREATED).body("{\"sucesso\": \"Conexao criada com sucesso!\"}");
    }
}
