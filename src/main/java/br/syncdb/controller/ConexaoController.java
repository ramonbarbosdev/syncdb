package br.syncdb.controller;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import br.syncdb.DTO.ConexaoDTO;
import br.syncdb.config.ConexaoBanco;
import br.syncdb.model.Conexao;
import br.syncdb.model.Usuario;
import br.syncdb.repository.ConexaoRepository;
import br.syncdb.utils.CriptoUtils;
import br.syncdb.utils.LeitorConfigSegura;

@RestController
@RequestMapping("/conexao")
public class ConexaoController {
    @Autowired
    private ConexaoRepository repository;

    @PostMapping(value = "/", produces = "application/json")
    public ResponseEntity<?> salvar(@RequestBody ConexaoDTO conexaoDTO) {

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

        return new ResponseEntity<Conexao>(conexaoModel, HttpStatus.OK);
    }

    @PutMapping(value = "/", produces = "application/json")
    public ResponseEntity<?> atualizar(@RequestBody ConexaoDTO conexaoDTO) {
        Optional<Conexao> conexaoModelOptional = repository.findById(conexaoDTO.getId_conexao());

        if (!conexaoModelOptional.isPresent()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("{\"erro\": \"Conexao não encontrada para atualizar!\"}");
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

        ConexaoBanco.fecharTodos();

        return new ResponseEntity<Conexao>(conexaoModel, HttpStatus.OK);
    }

    @GetMapping(value = "/", produces = "application/json")
    public ResponseEntity<?> recuperarConexao() {
        Conexao conexao = repository.buscarConexaoPorOrdem();

        if (conexao == null) {
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

    @PostMapping("/certificado/upload")
    public ResponseEntity<?> uploadCertificado(@RequestParam("arquivo") MultipartFile arquivo) {
        try {
            // byte[] chave = CriptoUtils.gerarChave256(System.getenv("SEGREDO_CONFIG"));
            String segredo = "wD7#G2k!91zL*qpB3VmX8eTR";

            byte[] chave = CriptoUtils.gerarChave256(segredo);
            String conteudo = new String(arquivo.getBytes(), StandardCharsets.UTF_8);

            String jsonDescriptografado = CriptoUtils.descriptografar(conteudo, chave);

            JSONObject obj = new JSONObject(jsonDescriptografado);

            // Opcional: validar campos esperados
            if (!obj.has("user") || !obj.has("password")) {
                return ResponseEntity.badRequest().body("Certificado inválido.");
            }

            ConexaoDTO.CloudConnection cloud = new ConexaoDTO.CloudConnection();
            cloud.setDb_cloud_host(obj.getString("host"));
            cloud.setDb_cloud_port(obj.getString("port"));
            cloud.setDb_cloud_user(obj.getString("user"));
            cloud.setDb_cloud_password(obj.getString("password"));

            System.out.println(cloud);


            // Aqui você pode salvar os dados na memória, cache, sessão ou arquivo
            // temporário
            return ResponseEntity.ok("Certificado válido e processado com sucesso.");

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body("Falha ao processar o certificado: " + e.getMessage());
        }
    }

    @GetMapping(value = "/certificado", produces = "application/json")
    public ResponseEntity<?> obterCertificado() throws Exception {

        String segredo = "wD7#G2k!91zL*qpB3VmX8eTR";

        Properties props = LeitorConfigSegura.carregarConfiguracao("./config.enc", segredo);

        ConexaoDTO.CloudConnection cloud = new ConexaoDTO.CloudConnection();
        cloud.setDb_cloud_host(props.getProperty("host"));
        cloud.setDb_cloud_port(props.getProperty("port"));
        cloud.setDb_cloud_user(props.getProperty("user"));
        cloud.setDb_cloud_password(props.getProperty("password"));

        System.out.println(cloud.getDb_cloud_password());

        return ResponseEntity.ok(props);
    }
}
