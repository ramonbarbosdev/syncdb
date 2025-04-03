package br.syncdb.utils;

import java.util.HashMap;
import java.util.Map;

public class DicionarioTipoSql {
    
     private static final Map<String, String> DICIONARIO_TIPOS = new HashMap<>();

     static {
        DICIONARIO_TIPOS.put("bigserial", "bigint");
        // DICIONARIO_TIPOS.put("serial", "integer");
        // DICIONARIO_TIPOS.put("smallserial", "smallint");
        // DICIONARIO_TIPOS.put("int8", "bigint");
        // DICIONARIO_TIPOS.put("int4", "integer");
        // DICIONARIO_TIPOS.put("int2", "smallint");
        // DICIONARIO_TIPOS.put("varchar", "text");
        // DICIONARIO_TIPOS.put("char", "text");
        // DICIONARIO_TIPOS.put("bpchar", "text"); 
        // DICIONARIO_TIPOS.put("float8", "double precision");
        // DICIONARIO_TIPOS.put("float4", "real");
        // DICIONARIO_TIPOS.put("numeric", "decimal");
        // DICIONARIO_TIPOS.put("bool", "boolean");
    }

    public static String getTipoConvertido(String tipo)
    {
        return DICIONARIO_TIPOS.getOrDefault(tipo.toLowerCase(), tipo);
    }
}
