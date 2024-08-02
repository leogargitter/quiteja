# quiteja

Funções para extração de dados, 

## Dependências

Usar o requirements.txt para construir o ambiente.

```
  pip install -r requirements.txt
```

## Extração de dados

![Extração gif](./readme_images/extract_data.gif)

Dentro da pasta do repositório executar o seguinte comando:

```
  python3 extract_data.py dados.zip
```

Isso executará o código para a extração dos dados do arquivo zip. Deixei como comando, pois é possível extrair qualquer .zip com arquivos que contenham uma estrutura parecida com o do teste.

## Query

![Query gif](./readme_images/query.gif)

Para executar a query é necessário ter rodado o script de extração de dados antes.

Dentro da pasta do repositório executar o seguinte comando:

```
  python3 query_data.py data.db
```

O comando irá printar o resultado da query.

## API Flask

Rodar o server com:

```
  run flask
```

A API deve ter o seguinte endpoint disponível:  *http://localhost:5000/tipo/<int>*
