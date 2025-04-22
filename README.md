# Sincronização de Banco de Dados

Este é um projeto para sincronização de bancos de dados entre duas instâncias (local e cloud). O objetivo principal deste sistema é garantir que as alterações feitas em uma instância do banco de dados sejam refletidas na outra, mantendo ambos os bancos de dados atualizados e consistentes.

## Funcionalidades

- **Sincronização entre Bancos**: Sincroniza tabelas e dados entre bancos de dados local e na nuvem.
- **Detecção de Diferenças**: Identifica alterações e diferenças na estrutura das tabelas.
- **Cache**: Implementa cache para otimizar o processo de sincronização e evitar consultas desnecessárias.
- **Logs**: Registra detalhes sobre cada operação de sincronização para auditoria e depuração.
- **Suporte a diferentes tipos de conexão**: Conexões com bancos de dados local e na nuvem.

## Tecnologias Utilizadas

- **Java**: Linguagem principal para o desenvolvimento.
- **JDBC**: Para conectar e realizar operações nos bancos de dados.
- **PostgreSQL**: Banco de dados utilizado na sincronização.
- **Spring Boot**: Framework para desenvolvimento do back-end.
- **Maven**: Gerenciamento de dependências e construção do projeto.
- **GitHub**: Repositório do código-fonte.

## Arquitetura

A arquitetura do sistema é baseada na comunicação entre dois bancos de dados, um local e outro na nuvem. O sistema é responsável por detectar diferenças de estrutura (como tabelas e colunas) e aplicar as alterações necessárias.

### Fluxo de Sincronização

1. **Verificação de Estrutura**: O sistema verifica se há diferenças entre as estruturas de tabelas dos dois bancos.
2. **Processamento das Diferenças**: O sistema gera as queries necessárias para sincronizar a estrutura (criação de tabelas, adição de colunas, etc.).
3. **Execução das Alterações**: As queries são executadas para atualizar a estrutura dos bancos.
4. **Armazenamento em Cache**: O sistema armazena as informações de sincronização em cache para melhorar o desempenho em futuras execuções.

## Como Usar

### Pré-requisitos

Antes de começar, você precisa garantir que tem os seguintes pré-requisitos instalados:

- Java 8 ou superior.
- Maven.
- Banco de dados PostgreSQL (ou outro banco de dados compatível com JDBC).

