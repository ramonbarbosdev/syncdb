package br.syncdb.excecoes;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;

import java.security.SignatureException;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import io.jsonwebtoken.ExpiredJwtException;
import jakarta.persistence.EntityNotFoundException;

import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;

@RestControllerAdvice
public class ControleExcecoes {

   
    @ExceptionHandler(ExpiredJwtException.class)
    public ResponseEntity<ObjetoErro> handleExpiredJwtException(ExpiredJwtException e) {

        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError("TOKEN expirado, faça um novo login!");
        objetoErro.setCode(HttpStatus.BAD_REQUEST.toString());

		System.out.println("Tipo da exceção: " + e.getClass().getName());


        return new ResponseEntity<>(objetoErro, HttpStatus.UNAUTHORIZED);
       
    }

    @ExceptionHandler(SignatureException.class)
    public ResponseEntity<ObjetoErro> handleSignatureException(SignatureException e) {
        
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError("Assinatura do token inválida!");
        objetoErro.setCode(HttpStatus.BAD_REQUEST.toString());

		System.out.println("Tipo da exceção: " + e.getClass().getName());

        return new ResponseEntity<>(objetoErro, HttpStatus.UNAUTHORIZED);
    }


    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ObjetoErro> handleValidationExceptions(MethodArgumentNotValidException ex) {
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError("Erro de validação: " + ex.getBindingResult().getAllErrors().get(0).getDefaultMessage());
        objetoErro.setCode(HttpStatus.BAD_REQUEST.toString());

		System.out.println("Tipo da exceção: " + ex.getClass().getName());

        return new ResponseEntity<>(objetoErro, HttpStatus.UNAUTHORIZED);

    }

    @ExceptionHandler(EntityNotFoundException.class)
    public ResponseEntity<ObjetoErro> handleEntityNotFoundException(EntityNotFoundException ex) {
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError("Entidade não encontrada: " + ex.getMessage());
        objetoErro.setCode(HttpStatus.NOT_FOUND.toString());

		System.out.println("Tipo da exceção: " + ex.getClass().getName());


        return new ResponseEntity<>(objetoErro, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({DataIntegrityViolationException.class, JpaSystemException.class})
    public ResponseEntity<ObjetoErro> handleDataIntegrityAndJpaExceptions(Exception ex) {
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError(MensagemException.tratamentoViolacaoIntegridadeDados(ex));
        objetoErro.setCode(HttpStatus.CONFLICT.toString());

		System.out.println("Tipo da exceção: " + ex.getClass().getName());


        return new ResponseEntity<>(objetoErro, HttpStatus.CONFLICT);
    }

    @ExceptionHandler(EmptyResultDataAccessException.class)
    public ResponseEntity<ObjetoErro> handleEmptyResultDataAccessException(EmptyResultDataAccessException ex) {
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError("Nenhum resultado encontrado: " + ex.getMessage());
        objetoErro.setCode(HttpStatus.NOT_FOUND.toString());

		System.out.println("Tipo da exceção: " + ex.getClass().getName());


        return new ResponseEntity<>(objetoErro, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(TransactionSystemException.class)
    public ResponseEntity<ObjetoErro> handleTransactionSystemException(TransactionSystemException ex) {
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError(MensagemException.tratamentoTransacaoSistema(ex));
        objetoErro.setCode(HttpStatus.INTERNAL_SERVER_ERROR.toString());

		System.out.println("Tipo da exceção: " + ex.getClass().getName());


        return new ResponseEntity<>(objetoErro, HttpStatus.INTERNAL_SERVER_ERROR);
    }

	@ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ObjetoErro> handleDeserializationException(HttpMessageNotReadableException ex) {
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError(MensagemException.tratamentoMensagemHTTPIlegivel(ex));
        objetoErro.setCode(HttpStatus.BAD_REQUEST.toString());

		System.out.println("Tipo da exceção: " + ex.getClass().getName());


        return new ResponseEntity<>(objetoErro, HttpStatus.BAD_REQUEST);
    }

    // @ExceptionHandler(NullPointerException.class)
    // public ResponseEntity<ObjetoErro> handlePointerException(Exception ex) {
    //     ObjetoErro objetoErro = new ObjetoErro();
    //     objetoErro.setError("Erro inesperado: " + ex.getMessage());
    //     objetoErro.setCode(HttpStatus.INTERNAL_SERVER_ERROR.toString());

	// 	System.out.println("Tipo da exceção: " + ex.getClass().getName());


    //     return new ResponseEntity<>(objetoErro, HttpStatus.INTERNAL_SERVER_ERROR);
    // }
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ObjetoErro> handleGenericException(Exception ex) {
        ObjetoErro objetoErro = new ObjetoErro();
        objetoErro.setError("Erro inesperado: " + ex.getMessage());
        objetoErro.setCode(HttpStatus.INTERNAL_SERVER_ERROR.toString());

		System.out.println("Tipo da exceção: " + ex.getClass().getName());


        return new ResponseEntity<>(objetoErro, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}