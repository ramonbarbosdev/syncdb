package br.syncdb.controller;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
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
    public ResponseEntity<?> salvar(@RequestBody ConexaoDTO conexaoDTO)
    {
    
        Conexao conexaoModel = new Conexao();

        conexaoModel.setDb_cloud_host(conexaoDTO.getCloud().getDb_cloud_host());
        conexaoModel.setDb_cloud_port(conexaoDTO.getCloud().getDb_cloud_port());
        conexaoModel.setDb_cloud_user(conexaoDTO.getCloud().getDb_cloud_user());
        conexaoModel.setDb_cloud_password(conexaoDTO.getCloud().getDb_cloud_password());

        conexaoModel.setDb_local_host(conexaoDTO.getLocal().getDb_local_host());
        conexaoModel.setDb_local_port(conexaoDTO.getLocal().getDb_local_port());
        conexaoModel.setDb_local_user(conexaoDTO.getLocal().getDb_local_user());
        conexaoModel.setDb_local_password(conexaoDTO.getLocal().getDb_local_password());

        repository.save(conexaoModel);

       return ResponseEntity.status(HttpStatus.CREATED).body("{\"sucesso\": \"Conexao criada com sucesso!\"}");
    }

    @PutMapping(value = "/", produces = "application/json")
    public ResponseEntity<?> atualizar(@RequestBody ConexaoDTO conexaoDTO)
    {
        Optional<Conexao> conexaoModelOptional = repository.findById(conexaoDTO.getId_conexao());
    
        if (!conexaoModelOptional.isPresent()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("{\"erro\": \"Conexao não encontrada para atualizar!\"}");
        }

        Conexao conexaoModel = conexaoModelOptional.get();

        conexaoModel.setDb_cloud_host(conexaoDTO.getCloud().getDb_cloud_host());
        conexaoModel.setDb_cloud_port(conexaoDTO.getCloud().getDb_cloud_port());
        conexaoModel.setDb_cloud_user(conexaoDTO.getCloud().getDb_cloud_user());
        conexaoModel.setDb_cloud_password(conexaoDTO.getCloud().getDb_cloud_password());

        conexaoModel.setDb_local_host(conexaoDTO.getLocal().getDb_local_host());
        conexaoModel.setDb_local_port(conexaoDTO.getLocal().getDb_local_port());
        conexaoModel.setDb_local_user(conexaoDTO.getLocal().getDb_local_user());
        conexaoModel.setDb_local_password(conexaoDTO.getLocal().getDb_local_password());

        repository.save(conexaoModel);

       return ResponseEntity.status(HttpStatus.OK).body("{\"sucesso\": \"Conexao criada com sucesso!\"}");
    }


    @GetMapping(value = "/", produces = "application/json")
    public ResponseEntity<?> recuperarConexao() {
        Conexao conexao = repository.buscarConexaoPorOrdem();

        if (conexao == null)
        {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("{\"erro\": \"Conexao não encontrada!\"}");
        }
    
        ConexaoDTO conexaoDTO = new ConexaoDTO();

        ConexaoDTO.CloudConnection cloud = new ConexaoDTO.CloudConnection();
        cloud.setDb_cloud_host(conexao.getDb_cloud_host());
        cloud.setDb_cloud_port(conexao.getDb_cloud_port());
        cloud.setDb_cloud_user(conexao.getDb_cloud_user());
        cloud.setDb_cloud_password(conexao.getDb_cloud_password());

        ConexaoDTO.LocalConnection local = new ConexaoDTO.LocalConnection();
        local.setDb_local_host(conexao.getDb_local_host());
        local.setDb_local_port(conexao.getDb_local_port());
        local.setDb_local_user(conexao.getDb_local_user());
        local.setDb_local_password(conexao.getDb_local_password());

        conexaoDTO.setId_conexao(conexao.getId_conexao());
        conexaoDTO.setCloud(cloud);
        conexaoDTO.setLocal(local);

        return ResponseEntity.ok(conexaoDTO);
    }
}
