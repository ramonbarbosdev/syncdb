package br.syncdb.excecoes;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.transaction.TransactionSystemException;

import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;

public class MensagemException extends RuntimeException{

	public MensagemException(String message)
	{
	        super(message);
	}

	 public static String tratamentoTransacaoSistema(TransactionSystemException ex)
    {
        Throwable rootCause = ex.getRootCause();
        
        if (rootCause instanceof ConstraintViolationException)
        {
            return refinarValidacaoErro((ConstraintViolationException) rootCause);
        }

        return "Erro ao processar a transação: " + (rootCause != null ? rootCause.getMessage() : ex.getMessage());
    }

    public static String refinarValidacaoErro(ConstraintViolationException ex)
    {
        StringBuilder errorMessage = new StringBuilder("Erro de validação: ");

        Set<ConstraintViolation<?>> violations = ex.getConstraintViolations();
        
        for (ConstraintViolation<?> violation : violations)
        {
            errorMessage.append(violation.getMessage()).append("; ");
        }
        
        return errorMessage.toString();
    }

	public static String tratamentoMensagemHTTPIlegivel(HttpMessageNotReadableException ex)
	{
        String message = ex.getMessage();

        if (message.contains("Cannot deserialize value of type"))
		{
	
            return "Erro de deserialização: O valor fornecido não é válido. ";
        }
        
        return  message;
    }

	public static String tratamentoViolacaoIntegridadeDados(Exception ex)
	{
        String message = ex.getMessage();
		
        if (message.contains("null value in column"))
		{
			String nmColuna = extrairNomeColuna(message);
            return "A coluna "+nmColuna+ " não pode ser nulo.";
        }

		if(message.contains("duplicate key value violates"))
		{
			//ex: Key (cd_conta)=(1) already exists
			String regex = "Key \\((.*?)\\)=\\((.*?)\\) already exists";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(message);

			if (matcher.find())
			{
				String chave = matcher.group(1); 
				String valor = matcher.group(2); 

				return "O valor '"+valor+"' já existe para o campo '"+chave+ "'. Por favor, escolha um valor diferente.";
			}
		
		}

		if(message.contains("still referenced from"))
		{

			//(.?) serve para pegar a informacao exata
			String regex = "table \"(.*?)\".]";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(message);


			if (matcher.find())
			{
				String tabelaReferencia = matcher.group(1); 
				
				if(message.contains("delete from"))
				{
					return "Não é possivel deletar o registro porque ele está referenciado na tabela '" + tabelaReferencia+ "'.";

				}
				
			}

		}
        
		
        return  message;
    }

	public static String extrairNomeColuna(String message) {
        String[] parts = message.split("\"");

        if (parts.length > 1) {
            return parts[1]; // Retorna o nome da coluna
        }
        return "campo desconhecido"; // Retorna um valor padrão se não encontrar
    }

	
	
}
