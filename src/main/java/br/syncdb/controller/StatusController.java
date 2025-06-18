package br.syncdb.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController 
@RequestMapping(value = "/status")
public class StatusController {
    
    @GetMapping(value = "/", produces = "application/json")
	public ResponseEntity<?> verificaStatus ( ) 
	{

		return new ResponseEntity<>(Map.of("status", true), HttpStatus.OK);
	}
}
