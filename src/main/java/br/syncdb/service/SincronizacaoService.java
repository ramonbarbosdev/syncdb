package br.syncdb.service;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import org.jooq.exception.DataAccessException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.github.benmanes.caffeine.cache.Cache;

import br.syncdb.config.ConexaoBanco;
import br.syncdb.controller.TipoConexao;
import br.syncdb.model.Coluna;
import br.syncdb.model.EstruturaTabela;
import br.syncdb.model.TabelaDetalhe;

@Service
public class SincronizacaoService
{
    @Autowired
    private EstruturaService estruturaService;

    @Autowired
    private DadosService dadosService;
    
    @Autowired
    private OperacaoBancoService operacaoBancoService;

    @Autowired
    private CacheService cacheService;

    
   
   
}
