# MC714 (Sistemas Distribuídos) - Trabalho 2

Implementação e avaliação experimental de algoritmos clássicos de sistemas distribuídos integrados em uma aplicação de liquidação bancária (`distrib-ledger`).

Autores:
- Celso Gabriel Ferreira Marçal Prado (RA: 235393)
- Giovanni Santos Scalabrin (RA: 281210)

Vídeo de demonstração: https://youtu.be/3dndSmKldBw

---

## Visão Geral e Algoritmos Implementados

O sistema modela um consórcio financeiro com nós independentes que se comunicam exclusivamente por sockets TCP assíncronos. Três algoritmos distribuídos operam em conjunto sobre a mesma aplicação:

1. **Exclusão Mútua Distribuída (Ricart-Agrawala)**: serializa o acesso à seção crítica (SC) pela ordem total definida por relógios lógicos de Lamport, com troca de `2(N-1)` mensagens em execuções sem perdas e retransmissão idempotente de pedidos em intervalos configuráveis sob perda de pacotes.
2. **Compromisso Atômico em Duas Fases (2PC) com Write-Ahead Log (WAL)**: coordena transferências entre nós com reserva prévia de fundos (evitando gasto duplo), persistência síncrona via `os.fsync`, registro `APPLIED` para idempotência na recuperação, regra de cancelamento presumido (*Presumed Abort*) quando o coordenador cai antes da decisão e consultas cooperativas entre participantes para transações *in-doubt*.
3. **Snapshot Distribuído (Chandy-Lamport, item de bônus)**: captura cortes globais consistentes sem interromper transferências concorrentes, utilizando um portão de ordenação por destino (`before_app_send`) para impedir que transferências emitidas após a captura local ultrapassem marcadores no canal. Um auditor verifica a conservação do capital total entre saldos locais e mensagens em trânsito.

---

## Estrutura do Repositório

```text
t2-mc714/
├── compose.yaml                   # Cluster de 5 nós (mc714-node1 a mc714-node5) via Docker Compose v2
├── Dockerfile                     # Imagem OCI Python 3.14 para os nós
├── Makefile                       # Atalhos para testes, experimentos, gráficos, relatório e verificação
├── README.md                      # Instruções de reprodução e uso
├── distrib_ledger/                # Código-fonte principal (sem dependências externas para os protocolos)
│   ├── node.py                    # Processo lógico do nó, reconstrução via WAL e plano de controle
│   ├── cli.py                     # CLI para executar nós e enviar comandos de controle
│   ├── network/
│   │   ├── protocol.py            # Enquadramento por prefixo de 4 bytes e serialização JSON
│   │   ├── channel.py             # Canal TCP assíncrono com ordem FIFO por destino e timeout de 2 s
│   │   └── fault_injector.py      # Injeção determinística de atraso, jitter, perda, partição e crash
│   ├── mutual_exclusion/
│   │   ├── lamport_clock.py       # Relógio lógico escalar de Lamport
│   │   └── ricart_agrawala.py     # Máquina de estados de Ricart-Agrawala e fila de diferimento
│   ├── atomic_commit/
│   │   ├── wal.py                 # Write-Ahead Log com fsync e truncamento de registros parciais
│   │   ├── two_phase_commit.py    # Coordenador e participantes do 2PC com retransmissão de decisão
│   │   └── recovery.py            # Recuperação no boot e protocolo cooperativo de terminação
│   └── snapshot/
│       ├── chandy_lamport.py      # Propagação de marcadores, portão de ordenação e gravação de canal
│       └── auditor.py             # Agregação de cortes e validação de conservação patrimonial
├── tests/                         # Suíte com 23 testes automatizados
│   ├── test_ricart_agrawala.py    # Segurança, vivacidade e contagem exata 2(N-1)
│   ├── test_two_phase_commit.py   # Commit, aborto, WAL truncado, idempotência e participante in-doubt
│   ├── test_chandy_lamport.py     # Snapshot em repouso, em trânsito e teste de estresse concorrente
│   ├── test_fault_tolerance.py    # Partições, perda de pacotes e falhas combinadas
│   ├── test_node_integration.py   # Reserva de fundos e proteção por seção crítica no nó
│   └── test_process_deployment.py # Teste fim a fim com nós em processos separados via TCP
├── scripts/
│   ├── run_experiments.py         # Orquestrador de benchmarks com nós em processos independentes
│   ├── generate_plots.py          # Geração das figuras em PDF e PNG (p50, p95, barras de erro)
│   └── verify_submission.py       # Verificação automática de arquivos, testes e número de páginas
├── data/                          # Dados brutos dos experimentos e metadados do ambiente
├── figures/                       # Figuras vetoriais (PDF) e PNG geradas a partir de data/
└── report/                        # Fonte LaTeX (IEEEtran) e PDF compilado (relatorio.pdf)
```

---

## Instruções de Reprodução

### Pré-requisitos
- Linux com Python 3.10 ou superior
- `pandas`, `numpy` e `matplotlib` (para geração dos gráficos em `figures/`)
- `pypdf` (opcional, para conferência do número de páginas em `verify_submission.py`)
- `pdflatex` (para compilar `report/relatorio.pdf`)
- Docker Engine e Docker Compose v2 (para executar o cluster em contêineres)

### 1. Executar a Suíte de 23 Testes Automatizados

```bash
make test
# ou:
python3 -m unittest discover -s tests -p "test_*.py" -v
```

### 2. Executar os Experimentos e Gerar Gráficos

Os experimentos iniciam cada nó como um processo independente (`python3 -m distrib_ledger.cli node`) comunicando-se por TCP e salvam os dados brutos em `data/`:

```bash
# Campanha completa (10 repetições por configuração)
make bench

# Execução rápida para validação (2 repetições)
make bench-quick

# Executar apenas um experimento específico
python3 scripts/run_experiments.py --only exp1 --keep-logs

# Gerar os gráficos em figures/
make plots
```

### 3. Compilar o Relatório IEEE

```bash
make report
# gera report/relatorio.pdf
```

### 4. Verificação Completa Antes da Entrega

```bash
make verify
```

---

## Execução do Cluster com Docker Compose v2

O arquivo `compose.yaml` sobe cinco nós (`mc714-node1` a `mc714-node5`, nas portas `8001` a `8005` da rede `mc714-net`, com volumes individuais em `./wal_data/` e `node1` como auditor padrão).

```bash
# Construir a imagem e iniciar os 5 contêineres
docker compose up --build -d

# Acompanhar os logs do cluster
docker compose logs -f

# Encerrar e remover contêineres e rede
docker compose down -v
```

### Operação pelo Plano de Controle (CLI)

Com o cluster em execução, é possível interagir com qualquer nó usando `distrib_ledger.cli client`:

```bash
# Verificar disponibilidade e estado/saldos de um nó
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 ping
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 stats

# Transferência atômica via 2PC protegida pela seção crítica de Ricart-Agrawala
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 transfer --to node2 --amount 150

# Gerar tráfego de fundo e executar snapshot + auditoria de Chandy-Lamport
python3 -m distrib_ledger.cli client --target 127.0.0.1:8002 load --to node3 --count 20 --rate 10 --amount 10
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 snapshot --id snap1
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 audit --id snap1

# Injeção de falhas (atraso, perda, partição, cicatrização e crash abrupto)
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 fault --partition node2 --seed 42
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 fault --heal
python3 -m distrib_ledger.cli client --target 127.0.0.1:8001 crash
```
