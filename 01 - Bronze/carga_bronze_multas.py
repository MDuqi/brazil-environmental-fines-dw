"""
Carga da camada bronze - Multas Ambientais (Ibama)

Lê todos os CSVs no padrão
    multas_ambientais_distribuidas_por_bens_tutelados_<UF>.csv
de uma pasta e carrega cada um em uma tabela própria, multas_<uf>,
dentro do schema bronze do PostgreSQL.

- A pasta dos CSVs e a conexão com o banco vêm do arquivo .env
  (que fica na mesma pasta deste script).
- A tabela é criada se não existir. Se já existir, o conteúdo é
  substituído (TRUNCATE + carga), então rodar de novo não duplica dados.
- Bronze = dado bruto: todas as colunas são gravadas como texto, sem
  conversão de tipos. Só os nomes das colunas são normalizados
  (minúsculas, sem acento/espaço) para facilitar as consultas.

Uso:
    python carga_bronze_multas.py
"""

import csv
import io
import logging
import os
import re
import sys
import unicodedata
import warnings
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import OperationalError

PADRAO_ARQUIVO = re.compile(
    r"^multas_ambientais_distribuidas_por_bens_tutelados_([A-Za-z]{2})\.csv$",
    re.IGNORECASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("carga_bronze")


# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
def carregar_config() -> dict:
    """Lê as variáveis do .env e valida as obrigatórias."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(env_path)

    obrigatorias = ["CSV_DIR", "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]
    faltando = [v for v in obrigatorias if not os.getenv(v)]
    if faltando:
        sys.exit(
            f"Variáveis faltando no .env ({env_path}): {', '.join(faltando)}\n"
            "Copie o .env.example para .env e preencha os valores."
        )

    return {
        "csv_dir": Path(os.environ["CSV_DIR"]).expanduser(),
        "db_host": os.environ["DB_HOST"],
        "db_port": int(os.environ["DB_PORT"]),
        "db_name": os.environ["DB_NAME"],
        "db_user": os.environ["DB_USER"],
        "db_password": os.environ["DB_PASSWORD"],
        "db_schema": os.getenv("DB_SCHEMA", "sch_bronze"),
        # Opcionais: se vazios, são detectados automaticamente
        "csv_sep": os.getenv("CSV_SEP") or None,
        "csv_encoding": os.getenv("CSV_ENCODING") or None,
        "chunk_size": int(os.getenv("CHUNK_SIZE") or 50_000),
    }


def criar_engine(cfg: dict) -> Engine:
    # URL.create trata senhas com caracteres especiais (@, :, / ...)
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=cfg["db_user"],
        password=cfg["db_password"],
        host=cfg["db_host"],
        port=cfg["db_port"],
        database=cfg["db_name"],
    )
    return create_engine(url)


# ---------------------------------------------------------------------------
# Leitura dos CSVs
# ---------------------------------------------------------------------------
def listar_arquivos(pasta: Path) -> list[tuple[str, Path]]:
    """Retorna [(uf, caminho), ...] para os CSVs que seguem o padrão."""
    if not pasta.is_dir():
        sys.exit(f"CSV_DIR não é uma pasta válida: {pasta}")

    encontrados = []
    for arq in sorted(pasta.iterdir()):
        if not arq.is_file() or arq.suffix.lower() != ".csv":
            continue
        m = PADRAO_ARQUIVO.match(arq.name)
        if m:
            encontrados.append((m.group(1).upper(), arq))
        else:
            log.warning("Ignorando %s (fora do padrão de nome)", arq.name)
    return encontrados


def normalizar_colunas(nomes: list[str]) -> list[str]:
    """'Valor da Multa (R$)' -> 'valor_da_multa_r'. Garante nomes únicos."""
    resultado, vistos = [], {}
    for i, nome in enumerate(nomes, start=1):
        s = unicodedata.normalize("NFKD", str(nome)).encode("ascii", "ignore").decode()
        s = re.sub(r"[^0-9a-zA-Z]+", "_", s).strip("_").lower()
        if not s or s.startswith("unnamed"):
            s = f"coluna_{i}"
        if s[0].isdigit():
            s = f"c_{s}"
        s = s[:60]  # limite de 63 caracteres do Postgres
        if s in vistos:
            vistos[s] += 1
            s = f"{s}_{vistos[s]}"
        else:
            vistos[s] = 1
        resultado.append(s)
    return resultado


def ler_cabecalho(arquivo: Path, encoding: str, sep: str | None) -> tuple[str, list[str]]:
    """Lê a 1ª linha, detecta o separador (se não informado) e devolve as colunas."""
    with open(arquivo, encoding=encoding, newline="") as f:
        primeira_linha = f.readline()
    if not sep:
        sep = max([";", ",", "\t", "|"], key=primeira_linha.count)
    colunas = next(csv.reader([primeira_linha], delimiter=sep))
    return sep, colunas


# ---------------------------------------------------------------------------
# Gravação no Postgres
# ---------------------------------------------------------------------------
def copy_postgres(table, conn, keys, data_iter):
    """
    Método de inserção para o DataFrame.to_sql usando COPY do Postgres,
    bem mais rápido que INSERT linha a linha (receita da doc do pandas).
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        buffer = io.StringIO()
        csv.writer(buffer).writerows(data_iter)
        buffer.seek(0)
        colunas = ", ".join(f'"{k}"' for k in keys)
        destino = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
        cur.copy_expert(f"COPY {destino} ({colunas}) FROM STDIN WITH CSV", buffer)


def relatar_avisos(arquivo: Path, avisos: list) -> None:
    """Resume no log as linhas puladas pelo pandas e repassa outros avisos."""
    puladas = []
    for a in avisos:
        if issubclass(a.category, pd.errors.ParserWarning):
            puladas += [l.strip() for l in str(a.message).splitlines() if l.strip()]
        else:
            log.warning("%s: %s", arquivo.name, a.message)
    if puladas:
        log.warning(
            "%s: %d linha(s) mal formada(s) ignorada(s). Ex.: %s",
            arquivo.name, len(puladas), " | ".join(puladas[:3]),
        )


def garantir_schema(engine: Engine, schema: str) -> None:
    if not inspect(engine).has_schema(schema):
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        log.info("Schema %s criado", schema)


def carregar_arquivo(engine: Engine, cfg: dict, uf: str, arquivo: Path) -> int:
    """Carrega um CSV inteiro em uma única transação. Retorna nº de linhas."""
    schema = cfg["db_schema"]
    tabela = f"multas_{uf.lower()}"
    encodings = [cfg["csv_encoding"]] if cfg["csv_encoding"] else ["utf-8-sig", "latin-1"]

    for encoding in encodings:
        try:
            sep, colunas_originais = ler_cabecalho(arquivo, encoding, cfg["csv_sep"])
            colunas = normalizar_colunas(colunas_originais)

            # Tudo numa transação: se der erro no meio, nada fica pela metade
            with engine.begin() as conn:
                if inspect(conn).has_table(tabela, schema=schema):
                    conn.execute(text(f'TRUNCATE TABLE "{schema}"."{tabela}"'))

                total = 0
                # Captura os avisos de linhas mal formadas para listar no log
                with warnings.catch_warnings(record=True) as avisos:
                    warnings.simplefilter("always")
                    leitor = pd.read_csv(
                        arquivo,
                        sep=sep,
                        encoding=encoding,
                        header=0,
                        names=colunas,
                        index_col=False,
                        dtype=str,               # bronze: tudo como texto
                        keep_default_na=False,   # não transforma "NA", "null" etc. em nulo
                        on_bad_lines="warn",     # pula linhas com campos a mais (e avisa)
                        chunksize=cfg["chunk_size"],
                    )
                    for chunk in leitor:
                        # if_exists="append" cria a tabela se ela não existir
                        chunk.to_sql(
                            tabela,
                            conn,
                            schema=schema,
                            if_exists="append",
                            index=False,
                            method=copy_postgres,
                        )
                        total += len(chunk)

                no_banco = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{schema}"."{tabela}"')
                ).scalar_one()

            relatar_avisos(arquivo, avisos)
            if no_banco != total:
                log.warning("%s: lidas %d linhas, mas há %d na tabela", tabela, total, no_banco)
            log.info(
                "%s -> %s.%s: %d linhas, %d colunas (encoding %s, separador %r)",
                arquivo.name, schema, tabela, no_banco, len(colunas), encoding, sep,
            )
            return no_banco

        except UnicodeDecodeError:
            log.info("%s não está em %s, tentando o próximo encoding...", arquivo.name, encoding)

    raise ValueError(f"não foi possível decodificar o arquivo com {encodings}")


# ---------------------------------------------------------------------------
def main() -> None:
    cfg = carregar_config()
    arquivos = listar_arquivos(cfg["csv_dir"])
    if not arquivos:
        sys.exit(f"Nenhum CSV no padrão encontrado em {cfg['csv_dir']}")
    log.info("%d arquivo(s) encontrado(s) em %s", len(arquivos), cfg["csv_dir"])

    engine = criar_engine(cfg)
    try:
        with engine.connect():
            pass
    except OperationalError as e:
        sys.exit(f"Não foi possível conectar ao Postgres. Confira os dados do .env.\n{e.orig}")
    garantir_schema(engine, cfg["db_schema"])

    sucesso, falhas = {}, {}
    for uf, arquivo in arquivos:
        try:
            sucesso[uf] = carregar_arquivo(engine, cfg, uf, arquivo)
        except Exception as e:  # segue para o próximo arquivo
            falhas[uf] = str(e).splitlines()[0]
            log.error("Falha ao carregar %s: %s", arquivo.name, falhas[uf])

    engine.dispose()

    log.info("=" * 60)
    log.info(
        "Concluído: %d tabela(s) carregada(s), %d linhas no total",
        len(sucesso), sum(sucesso.values()),
    )
    if falhas:
        log.error("Falharam: %s", ", ".join(falhas))
        sys.exit(1)


if __name__ == "__main__":
    main()
