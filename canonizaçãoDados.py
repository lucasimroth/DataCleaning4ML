from urllib.parse import urlparse

import pandas as pd

##função para parsear a url da linha e separaar entre path e query
def limpeza_url(url):
    url = url.replace(' HTTP/1.1', '').strip()
    parsed = urlparse(url)

    path = parsed.path
    args = parsed.query

    return path, args

##separando o dataset colocando só os headers utilizavei
def construtor_headers(row):
    parts = []

    if row['User-Agent']:
        parts.append(f"user-agent={row['User-Agent']}")
    if row['cookie']:
        parts.append(f"cookie={row['cookie']}")
    if row['content-type']:
        parts.append(f"content-type={row['content-type']}")
    if row['Accept']:
        parts.append(f"accept={row['Accept']}")
    if row['host']:
        parts.append(f"host={row['host']}")

    return " ".join(parts)

##função de canonização para converter os dados no padrão unico
def build_canonical_request(row):
    return (
        f"method:{row['Method']} | "
        f"url:{row['path']} | "
        f"args:{row['args']} | "
        f"form:{row['form']} | "
        f"headers:{row['headers_clean']} | "
        f"body:{row['body']} | "
        f"json:{row['json']}"
    )

file_path = "datasets/raw/csic_database.csv"

df = pd.read_csv(file_path)

##visualização do csv
##print(df.shape)
##print(df.columns)

##drop da coluna inutil
df = df.drop(columns=['Unnamed: 0'], errors='ignore')
##print(df.shape)
##print(df.columns)

##na diferença entre post e get algumas colunas ficam vazias em get como Nan, entao vou deixar como campo vazio escrito
df = df.fillna('')

##print(df['URL'].iloc[0])

##separando o pach e o query da URL para melhorar generalização
##path é o endpoint limpo
##args são os parametros
df[['path', 'args']] = df['URL'].apply(
    lambda x: pd.Series(limpeza_url(x))
)

##chamo a função mandando axis 1(linhas) para adicionar em cada linha a coluna respectiva do header_clean
df['headers_clean'] = df.apply(construtor_headers, axis=1)

##print(df['headers_clean'].iloc[0])

##mudando de body para content
df['body'] = df['content'].fillna('')

##retirar lixo do GET
df.loc[df['Method'] == 'GET', 'body'] = ''


##print(df[['Method','path', 'args', 'headers_clean','body']].head())

##colocando os campos necesarios para padronização
df['form'] = ''

df['json'] = ''

df['canonical_request'] = df.apply(build_canonical_request, axis=1)

print(df['canonical_request'].head(10).to_string(index=False))

df[['canonical_request', 'classification']].to_csv(
    "csic_canonical.csv",
    index=False
)
