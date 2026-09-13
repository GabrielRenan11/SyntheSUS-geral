# SyntheSUS Front-end

Front-end estático (HTML + CSS + JavaScript) para consumir a API:

`https://synthesus-api.onrender.com`

## Arquivos

- `index.html` — estrutura das seis telas.
- `styles.css` — identidade visual, sidebar, cards, filtros, rankings e responsividade.
- `app.js` — integração com a API, filtros dependentes e gráficos Chart.js.

## Bibliotecas

O projeto usa CDN, portanto não precisa de `npm install`:

- Bootstrap 5
- Bootstrap Icons
- Chart.js

## Como executar localmente

No diretório do front-end:

```powershell
python -m http.server 5500
```

Depois abra:

`http://localhost:5500`

Evite abrir o `index.html` diretamente com `file://`; usar um pequeno servidor local simplifica testes e comportamento do navegador.

## API

A constante da API fica no topo de `app.js`:

```js
const API_BASE = "https://synthesus-api.onrender.com";
```

Se a URL mudar, altere somente essa linha.

## Logo

O canto superior esquerdo já possui uma marca vetorial/CSS no estilo visual do SyntheSUS. Se quiser usar o arquivo oficial do logo, substitua o bloco `.brand-mark` do HTML por uma `<img>` e ajuste em `styles.css`.

## Observações funcionais

- Internações usam intervalo diário.
- Leitos, equipamentos, profissionais e parte de unidades usam competências mensais.
- Cards de inventário usam a competência final, evitando somar fotografias mensais.
- Dropdowns de unidade só são carregados sob demanda. Por desempenho, selecione primeiro uma UF ou município.
- Os gráficos são recriados quando o usuário muda filtros.
- Os filtros de `sexo`, `raça/cor`, `etnia` e `caráter` são texto livre para respeitar exatamente os valores existentes no Oracle.
