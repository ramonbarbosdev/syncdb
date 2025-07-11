package br.syncdb.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DicionarioTipoSql {

    private static final Map<String, String> DICIONARIO_TIPOS;

    static {
        Map<String, String> tipos = new HashMap<>();

        tipos.put("int2", "smallint");
        tipos.put("int4", "integer");
        tipos.put("int8", "bigint");

        tipos.put("serial", "integer");
        tipos.put("bigserial", "bigint");
        tipos.put("smallserial", "smallint");

        tipos.put("float4", "real");
        tipos.put("float8", "double precision");
        tipos.put("numeric", "decimal");
        tipos.put("money", "decimal");

        tipos.put("bool", "boolean");

        tipos.put("varchar", "varchar");
        tipos.put("char", "char");
        tipos.put("bpchar", "char");  // blank-padded char
        tipos.put("text", "text");

        tipos.put("uuid", "uuid");

        tipos.put("date", "date");
        tipos.put("timestamp", "timestamp without time zone");
        tipos.put("timestamptz", "timestamp with time zone");

        DICIONARIO_TIPOS = Collections.unmodifiableMap(tipos);
    }

    /**
     * Converte o tipo interno do PostgreSQL para um tipo padrão legível no DDL.
     *
     * @param tipo tipo retornado pelo JDBC ou pelo information_schema
     * @return tipo mapeado (ou o mesmo se não houver correspondência)
     */
    public static String getTipoConvertido(String tipo) {
        if (tipo == null) return "text";
        return DICIONARIO_TIPOS.getOrDefault(tipo.toLowerCase(), tipo);
    }
}